// kafka_db_writer.cpp
//
// Consumes green/pink sensor batches from Kafka (topics written by
// receiver.cpp) and persists them as:
//
//   - a per-stream append-only binary file holding the OpenZL-compressed
//     payload of every batch back to back (data/green.bin, data/pink.bin)
//   - a SQLite row per batch recording where that batch lives in the
//     binary file and what time range it covers:
//
//       CREATE TABLE batches (
//         id            INTEGER PRIMARY KEY AUTOINCREMENT,
//         address       TEXT    NOT NULL,   -- stream name, e.g. "green"/"pink"
//         begin_index   INTEGER NOT NULL,   -- byte offset of this batch in the .bin file
//         end_index     INTEGER NOT NULL,   -- byte offset just past this batch
//         initial_time  INTEGER NOT NULL,   -- epoch ms of first row in the batch
//         end_time      INTEGER NOT NULL    -- epoch ms of last row in the batch
//       );
//
// To read a batch back later: look up its row, open the stream's .bin
// file, seek to begin_index, read (end_index - begin_index) bytes, and
// run the same OpenZL decompress used in the old receiver.cpp to get
// back the SensorRow columns.
//
// STREAM ISOLATION: green and pink each get their own Kafka consumer,
// own .bin file, and own SQLite connection (WAL mode), running on their
// own std::thread. There is no shared queue, lock, or file between them,
// so a slow disk write or a burst on one stream cannot delay the other.
// SQLite WAL still serializes the (very short) INSERT transactions
// against each other, but that's sub-millisecond and never blocks the
// much larger compress/append work happening concurrently.
//
// Build deps (vcpkg): librdkafka, sqlite3, plus your OpenZL build.
//   vcpkg install librdkafka sqlite3
//
// NOTE: compressColumns() below mirrors decompressColumns() from the
// original receiver.cpp (which used ZL_DCtx_decompressMultiTBuffer) but
// for the forward/compress direction. Double check the exact OpenZL
// compress entry points (ZL_CCtx_create / ZL_CCtx_compressMultiTBuffer
// or whatever your installed OpenZL version calls them) against your
// headers -- I don't have zl_compress.h to confirm exact symbol names.

#include <windows.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>
#include <memory>
#include <mutex>

#include "wire_protocol.h"

#include <librdkafka/rdkafkacpp.h>
#include <sqlite3.h>

#include "openzl/zl_compress.h"   // ZL_CCtx_create, ZL_CCtx_compressMultiTBuffer, etc.
#include "openzl/zl_compressor.h"
#include "openzl/zl_decompress.h" // kept in case you want to sanity-check on read-back
#include "openzl/zl_errors.h"

using namespace phonepipe;

namespace {

// ---------------------------------------------------------------------------
// OpenZL compress: takes the delta-encoded raw columnar payload (exactly
// what the phone sends when it can't/won't compress) and produces the
// same compressed format decompressColumns() in the original receiver.cpp
// expects to read back.
// ---------------------------------------------------------------------------
std::vector<uint8_t> compressColumns(const std::vector<uint8_t>& rawPayload, uint32_t rowCount) {
    const size_t colBytes = static_cast<size_t>(rowCount) * kColWidth;
    const size_t expectedSize = static_cast<size_t>(kFieldCount) * colBytes;

    if (rawPayload.size() != expectedSize) {
        fprintf(stderr, "compressColumns: unexpected payload size %zu (expected %zu)\n",
                rawPayload.size(), expectedSize);
        return {};
    }

    ZL_Compressor* compressor = ZL_Compressor_create();
    if (!compressor) {
        fprintf(stderr, "OpenZL compressor creation failed\n");
        return {};
    }

    ZL_Report report = ZL_Compressor_setParameter(
        compressor, ZL_CParam_formatVersion, ZL_MAX_FORMAT_VERSION);
    if (ZL_isError(report)) {
        fprintf(stderr, "OpenZL format setup failed: %s\n",
                ZL_Compressor_getErrorContextString(compressor, report));
        ZL_Compressor_free(compressor);
        return {};
    }

    report = ZL_Compressor_selectStartingGraphID(
        compressor, ZL_GRAPH_COMPRESS_GENERIC);
    if (ZL_isError(report)) {
        fprintf(stderr, "OpenZL graph setup failed: %s\n",
                ZL_Compressor_getErrorContextString(compressor, report));
        ZL_Compressor_free(compressor);
        return {};
    }

    ZL_CCtx* cctx = ZL_CCtx_create();
    if (!cctx) {
        fprintf(stderr, "OpenZL compression context creation failed\n");
        ZL_Compressor_free(compressor);
        return {};
    }

    report = ZL_CCtx_refCompressor(cctx, compressor);
    if (ZL_isError(report)) {
        fprintf(stderr, "OpenZL compressor attachment failed: %s\n",
                ZL_CCtx_getErrorContextString(cctx, report));
        ZL_CCtx_free(cctx);
        ZL_Compressor_free(compressor);
        return {};
    }

    // Match the app: typed numeric references must point to 8-byte-aligned
    // storage. The payload is already delta-encoded by the app.
    std::vector<int64_t> alignedData(expectedSize / sizeof(int64_t));
    std::memcpy(alignedData.data(), rawPayload.data(), expectedSize);

    std::vector<ZL_TypedRef*> inputs(kFieldCount, nullptr);
    for (int c = 0; c < kFieldCount; ++c) {
        const size_t offset = static_cast<size_t>(c) * colBytes;
        inputs[c] = ZL_TypedRef_createNumeric(
            reinterpret_cast<uint8_t*>(alignedData.data()) + offset,
            sizeof(int64_t),
            rowCount);
        if (!inputs[c]) {
            fprintf(stderr, "OpenZL typed input creation failed for column %d\n", c);
            for (auto* ref : inputs) {
                if (ref) ZL_TypedRef_free(ref);
            }
            ZL_CCtx_free(cctx);
            ZL_Compressor_free(compressor);
            return {};
        }
    }

    // Generous upper bound for the output buffer; OpenZL reports the
    // actual compressed size in the returned report.
    const size_t dstCapacity = expectedSize + expectedSize / 2 + 4096;
    std::vector<uint8_t> out(dstCapacity);
    std::vector<const ZL_TypedRef*> typedInputs;
    typedInputs.reserve(inputs.size());
    for (const auto* ref : inputs) {
        typedInputs.push_back(ref);
    }

    report = ZL_CCtx_compressMultiTypedRef(
        cctx,
        out.data(), out.size(),
        typedInputs.data(), typedInputs.size());

    std::vector<uint8_t> result;
    if (ZL_isError(report)) {
        fprintf(stderr, "OpenZL compress failed: %s\n",
                ZL_CCtx_getErrorContextString(cctx, report));
    } else {
        const size_t writtenSize = ZL_validResult(report);
        result.assign(out.begin(), out.begin() + writtenSize);
    }

    for (auto* ref : inputs) {
        if (ref) ZL_TypedRef_free(ref);
    }
    ZL_CCtx_free(cctx);
    ZL_Compressor_free(compressor);

    return result;
}

// ---------------------------------------------------------------------------
// SQLite: one connection per stream thread, WAL mode so green/pink inserts
// don't block each other's readers, short busy-timeout as a safety net
// against the brief writer-vs-writer lock.
// ---------------------------------------------------------------------------
sqlite3* openDb(const std::string& path) {
    sqlite3* db = nullptr;
    if (sqlite3_open(path.c_str(), &db) != SQLITE_OK) {
        fprintf(stderr, "Failed to open SQLite db %s: %s\n", path.c_str(), sqlite3_errmsg(db));
        return nullptr;
    }

    sqlite3_busy_timeout(db, 5000);

    const char* pragmas[] = {
        "PRAGMA journal_mode=WAL;",
        "PRAGMA synchronous=NORMAL;",
    };
    for (const char* p : pragmas) {
        char* err = nullptr;
        if (sqlite3_exec(db, p, nullptr, nullptr, &err) != SQLITE_OK) {
            fprintf(stderr, "SQLite pragma failed (%s): %s\n", p, err);
            sqlite3_free(err);
        }
    }

    const char* createTable =
        "CREATE TABLE IF NOT EXISTS batches ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  address TEXT NOT NULL,"
        "  begin_index INTEGER NOT NULL,"
        "  end_index INTEGER NOT NULL,"
        "  initial_time INTEGER NOT NULL,"
        "  end_time INTEGER NOT NULL"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_batches_address_time "
        "  ON batches(address, initial_time);";

    char* err = nullptr;
    if (sqlite3_exec(db, createTable, nullptr, nullptr, &err) != SQLITE_OK) {
        fprintf(stderr, "Failed to create table: %s\n", err);
        sqlite3_free(err);
        sqlite3_close(db);
        return nullptr;
    }

    return db;
}

bool insertBatchRow(sqlite3* db, const std::string& address,
                     int64_t beginIndex, int64_t endIndex,
                     int64_t initialTime, int64_t endTime) {
    static const char* sql =
        "INSERT INTO batches (address, begin_index, end_index, initial_time, end_time) "
        "VALUES (?, ?, ?, ?, ?);";

    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        fprintf(stderr, "SQLite prepare failed: %s\n", sqlite3_errmsg(db));
        return false;
    }

    sqlite3_bind_text(stmt, 1, address.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 2, beginIndex);
    sqlite3_bind_int64(stmt, 3, endIndex);
    sqlite3_bind_int64(stmt, 4, initialTime);
    sqlite3_bind_int64(stmt, 5, endTime);

    const bool ok = sqlite3_step(stmt) == SQLITE_DONE;
    if (!ok) {
        fprintf(stderr, "SQLite insert failed: %s\n", sqlite3_errmsg(db));
    }

    sqlite3_finalize(stmt);
    return ok;
}

// ---------------------------------------------------------------------------
// Per-stream worker: owns its Kafka consumer, its .bin file, and its
// SQLite connection. Fully independent of the other stream's worker.
// ---------------------------------------------------------------------------
class StreamWriter {
public:
    StreamWriter(uint8_t streamId, const std::string& brokers, const std::string& dataDir)
        : streamId_(streamId), name_(streamName(streamId)) {

        binPath_ = dataDir + "\\" + name_ + ".bin";
        dbPath_  = dataDir + "\\" + name_ + ".sqlite3";

        db_ = openDb(dbPath_);

        binFile_ = fopen(binPath_.c_str(), "ab+");
        if (!binFile_) {
            fprintf(stderr, "Failed to open %s\n", binPath_.c_str());
        }

        std::string errstr;
        std::unique_ptr<RdKafka::Conf> conf(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
        conf->set("bootstrap.servers", brokers, errstr);
        conf->set("group.id", "phonepipe-db-writer-" + name_, errstr);
        conf->set("enable.auto.commit", "true", errstr);
        conf->set("auto.offset.reset", "earliest", errstr);

        consumer_.reset(RdKafka::KafkaConsumer::create(conf.get(), errstr));
        if (!consumer_) {
            fprintf(stderr, "Failed to create consumer for %s: %s\n", name_.c_str(), errstr.c_str());
            return;
        }

        RdKafka::ErrorCode err = consumer_->subscribe({kafkaTopicFor(streamId_)});
        if (err != RdKafka::ERR_NO_ERROR) {
            fprintf(stderr, "Subscribe failed for %s: %s\n", name_.c_str(), RdKafka::err2str(err).c_str());
        }
    }

    ~StreamWriter() {
        if (consumer_) {
            consumer_->close();
        }
        if (binFile_) fclose(binFile_);
        if (db_) sqlite3_close(db_);
    }

    bool ok() const { return consumer_ && binFile_ && db_; }

    // Runs forever, consuming this stream's topic. Meant to be launched
    // on its own std::thread; blocks only on ITS OWN socket/disk/db, never
    // on the other stream's.
    void run() {
        while (true) {
            std::unique_ptr<RdKafka::Message> msg(consumer_->consume(1000 /*ms*/));

            switch (msg->err()) {
                case RdKafka::ERR__TIMED_OUT:
                    continue;
                case RdKafka::ERR_NO_ERROR:
                    handleMessage(static_cast<const uint8_t*>(msg->payload()), msg->len());
                    continue;
                case RdKafka::ERR__PARTITION_EOF:
                    continue;
                default:
                    fprintf(stderr, "[%s] Kafka consume error: %s\n", name_.c_str(), msg->errstr().c_str());
                    continue;
            }
        }
    }

private:
    void handleMessage(const uint8_t* raw, size_t len) {
        if (len < kHeaderSize) {
            fprintf(stderr, "[%s] short Kafka message (%zu bytes)\n", name_.c_str(), len);
            return;
        }

        const PacketHeader hdr = decodeHeader(raw);
        const uint8_t* payloadPtr = raw + kHeaderSize;
        const size_t payloadLen = len - kHeaderSize;

        if (payloadLen != hdr.payloadLen) {
            fprintf(stderr, "[%s] payload length mismatch: header says %u, got %zu\n",
                    name_.c_str(), hdr.payloadLen, payloadLen);
            return;
        }

        std::vector<uint8_t> compressedBytes;

        if (hdr.compressed) {
            // Already OpenZL-compressed on the phone -- store as-is.
            compressedBytes.assign(payloadPtr, payloadPtr + payloadLen);
        } else {
            // Raw (delta-encoded) columnar payload -- compress here so the
            // binary file always holds a uniformly compressed stream.
            std::vector<uint8_t> raw(payloadPtr, payloadPtr + payloadLen);
            compressedBytes = compressColumns(raw, hdr.rowCount);
            if (compressedBytes.empty()) {
                fprintf(stderr, "[%s] compress-on-ingest failed, dropping batch\n", name_.c_str());
                return;
            }
        }

        // Append to this stream's binary file and record the byte range.
        if (fseek(binFile_, 0, SEEK_END) != 0) {
            fprintf(stderr, "[%s] seek failed on %s\n", name_.c_str(), binPath_.c_str());
            return;
        }

        const long beginIndex = ftell(binFile_);
        if (beginIndex < 0) {
            fprintf(stderr, "[%s] ftell failed on %s\n", name_.c_str(), binPath_.c_str());
            return;
        }

        const size_t written = fwrite(compressedBytes.data(), 1, compressedBytes.size(), binFile_);
        if (written != compressedBytes.size()) {
            fprintf(stderr, "[%s] short write to %s\n", name_.c_str(), binPath_.c_str());
            return;
        }
        fflush(binFile_);

        const long endIndex = beginIndex + static_cast<long>(written);

        if (!insertBatchRow(db_, name_, beginIndex, endIndex, hdr.initialTimeMs, hdr.endTimeMs)) {
            fprintf(stderr, "[%s] DB insert failed for batch [%ld, %ld)\n", name_.c_str(), beginIndex, endIndex);
            return;
        }

        printf("[%s] stored %zu compressed bytes at [%ld, %ld), t=[%lld..%lld]\n",
               name_.c_str(), compressedBytes.size(), beginIndex, endIndex,
               static_cast<long long>(hdr.initialTimeMs),
               static_cast<long long>(hdr.endTimeMs));
    }

    uint8_t streamId_;
    std::string name_;
    std::string binPath_;
    std::string dbPath_;

    FILE* binFile_ = nullptr;
    sqlite3* db_ = nullptr;
    std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
};

} // namespace

int main() {
    std::string brokers = "localhost:9092";
    if (const char* envBrokers = std::getenv("KAFKA_BROKERS")) {
        brokers = envBrokers;
    }

    std::string dataDir = "data";
    CreateDirectoryA(dataDir.c_str(), nullptr); // ok if it already exists

    StreamWriter greenWriter(STREAM_GREEN, brokers, dataDir);
    StreamWriter pinkWriter(STREAM_PINK, brokers, dataDir);

    if (!greenWriter.ok() || !pinkWriter.ok()) {
        fprintf(stderr, "Failed to initialize one or both stream writers\n");
        return 1;
    }

    // Two independent threads, two independent consumers, two independent
    // files/DBs -- green and pink never wait on each other.
    std::thread greenThread([&greenWriter] { greenWriter.run(); });
    std::thread pinkThread([&pinkWriter] { pinkWriter.run(); });

    greenThread.join();
    pinkThread.join();

    return 0;
}
