// kafka_db_writer.cpp
//
// Consumes sensor batches from Kafka and writes decoded rows to SQLite only.
// This path deliberately drops the older binary-file staging approach.

#include <windows.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <memory>

#include "wire_protocol.h"
#include "gzip_codec.h"
#include "openzl_codec.h"

#include <librdkafka/rdkafkacpp.h>
#include <sqlite3.h>

#include "openzl/zl_decompress.h"
#include "openzl/zl_errors.h"

using namespace phonepipe;

namespace {

struct SensorRow {
    int64_t timestamp = 0;
    int64_t temp = 0;
    int64_t pressure = 0;
    int64_t flowRate = 0;
    int64_t massFlow = 0;
    int64_t volumeFlow = 0;
    int64_t density = 0;
    int64_t currentOfMotor = 0;
    int64_t percentageOfValve = 0;
};

void deltaDecodeInPlace(int64_t* col, int rowCount) {
    for (int i = 1; i < rowCount; ++i) {
        col[i] = col[i] + col[i - 1];
    }
}

std::vector<SensorRow> parseRawColumnar(const uint8_t* data, size_t len, uint32_t rowCount) {
    const size_t expectedBytes = static_cast<size_t>(kFieldCount) * kColWidth * rowCount;
    if (len != expectedBytes) {
        fprintf(stderr, "parseRawColumnar: invalid payload size %zu (expected %zu for %u rows)\n",
                len, expectedBytes, rowCount);
        return {};
    }

    std::vector<int64_t> rawData(reinterpret_cast<const int64_t*>(data),
                                 reinterpret_cast<const int64_t*>(data + len));
    const size_t colBytes = static_cast<size_t>(rowCount) * kColWidth;
    for (int c = 0; c < kFieldCount; ++c) {
        int64_t* col = rawData.data() + static_cast<size_t>(c) * rowCount;
        deltaDecodeInPlace(col, static_cast<int>(rowCount));
    }

    std::vector<SensorRow> rows(rowCount);
    const int64_t* timestamps = rawData.data();
    const int64_t* temp = timestamps + rowCount;
    const int64_t* pressure = temp + rowCount;
    const int64_t* flowRate = pressure + rowCount;
    const int64_t* massFlow = flowRate + rowCount;
    const int64_t* volumeFlow = massFlow + rowCount;
    const int64_t* density = volumeFlow + rowCount;
    const int64_t* currentOfMotor = density + rowCount;
    const int64_t* percentageOfValve = currentOfMotor + rowCount;

    for (uint32_t i = 0; i < rowCount; ++i) {
        rows[i].timestamp = timestamps[i];
        rows[i].temp = temp[i];
        rows[i].pressure = pressure[i];
        rows[i].flowRate = flowRate[i];
        rows[i].massFlow = massFlow[i];
        rows[i].volumeFlow = volumeFlow[i];
        rows[i].density = density[i];
        rows[i].currentOfMotor = currentOfMotor[i];
        rows[i].percentageOfValve = percentageOfValve[i];
    }
    return rows;
}

std::vector<SensorRow> decodeOpenZL(const std::vector<uint8_t>& compressed, uint32_t rowCount) {
    if (rowCount == 0 || compressed.empty()) return {};

    ZL_DCtx* dctx = ZL_DCtx_create();
    if (!dctx) {
        fprintf(stderr, "OpenZL decode context creation failed\n");
        return {};
    }

    std::vector<ZL_TypedBuffer*> outs(kFieldCount, nullptr);
    for (auto*& out : outs) {
        out = ZL_TypedBuffer_create();
        if (!out) {
            fprintf(stderr, "OpenZL typed buffer creation failed\n");
            for (auto* item : outs) {
                if (item) ZL_TypedBuffer_free(item);
            }
            ZL_DCtx_free(dctx);
            return {};
        }
    }

    const ZL_Report report = ZL_DCtx_decompressMultiTBuffer(
        dctx,
        outs.data(),
        outs.size(),
        compressed.data(),
        compressed.size());

    if (ZL_isError(report)) {
        fprintf(stderr, "OpenZL decode failed: %s\n",
                ZL_DCtx_getErrorContextString(dctx, report));
        for (auto* item : outs) {
            if (item) ZL_TypedBuffer_free(item);
        }
        ZL_DCtx_free(dctx);
        return {};
    }

    auto col = [&](int idx) {
        return static_cast<const uint8_t*>(ZL_TypedBuffer_rPtr(outs[idx]));
    };

    const size_t colBytes = static_cast<size_t>(rowCount) * kColWidth;
    std::vector<int64_t> flat(static_cast<size_t>(kFieldCount) * rowCount);
    size_t offset = 0;
    for (int c = 0; c < kFieldCount; ++c) {
        const auto* src = reinterpret_cast<const int64_t*>(col(c));
        std::memcpy(flat.data() + offset, src, colBytes);
        int64_t* dest = flat.data() + offset;
        deltaDecodeInPlace(dest, static_cast<int>(rowCount));
        offset += rowCount;
    }

    std::vector<SensorRow> rows(rowCount);
    const int64_t* timestamps = flat.data();
    const int64_t* temp = timestamps + rowCount;
    const int64_t* pressure = temp + rowCount;
    const int64_t* flowRate = pressure + rowCount;
    const int64_t* massFlow = flowRate + rowCount;
    const int64_t* volumeFlow = massFlow + rowCount;
    const int64_t* density = volumeFlow + rowCount;
    const int64_t* currentOfMotor = density + rowCount;
    const int64_t* percentageOfValve = currentOfMotor + rowCount;

    for (uint32_t i = 0; i < rowCount; ++i) {
        rows[i].timestamp = timestamps[i];
        rows[i].temp = temp[i];
        rows[i].pressure = pressure[i];
        rows[i].flowRate = flowRate[i];
        rows[i].massFlow = massFlow[i];
        rows[i].volumeFlow = volumeFlow[i];
        rows[i].density = density[i];
        rows[i].currentOfMotor = currentOfMotor[i];
        rows[i].percentageOfValve = percentageOfValve[i];
    }

    for (auto* item : outs) {
        if (item) ZL_TypedBuffer_free(item);
    }
    ZL_DCtx_free(dctx);
    return rows;
}

std::vector<SensorRow> decodePayload(const PacketHeader& hdr, const uint8_t* payloadPtr, size_t payloadLen) {
    if (hdr.flag == CompressionFlag::OPENZL) {
        const std::vector<uint8_t> compressed(payloadPtr, payloadPtr + payloadLen);
        return decodeOpenZL(compressed, hdr.rowCount);
    }

    if (hdr.flag == CompressionFlag::RAW) {
        return parseRawColumnar(payloadPtr, payloadLen, hdr.rowCount);
    }

    if (hdr.flag == CompressionFlag::GZIP) {
        const std::vector<uint8_t> raw = gzipDecompress(payloadPtr, payloadLen);
        if (raw.empty()) {
            fprintf(stderr, "gzip decompress failed for stream %u\n", hdr.streamId);
            return {};
        }
        return parseRawColumnar(raw.data(), raw.size(), hdr.rowCount);
    }

    fprintf(stderr, "unknown effective compression flag %u for stream %u\n",
            static_cast<unsigned>(hdr.flag), hdr.streamId);
    return {};
}

sqlite3* openDb(const std::string& path) {
    sqlite3* db = nullptr;
    if (sqlite3_open(path.c_str(), &db) != SQLITE_OK) {
        fprintf(stderr, "Failed to open SQLite db %s: %s\n", path.c_str(), sqlite3_errmsg(db));
        return nullptr;
    }

    sqlite3_busy_timeout(db, 5000);
    sqlite3_exec(db, "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;", nullptr, nullptr, nullptr);

    static const char* newSchema =
        "CREATE TABLE IF NOT EXISTS sensor_rows ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  stream_id INTEGER NOT NULL,"
        "  address TEXT NOT NULL,"
        "  row_count INTEGER NOT NULL DEFAULT 0,"
        "  received_begin_time INTEGER NOT NULL DEFAULT 0,"
        "  received_end_time INTEGER NOT NULL DEFAULT 0,"
        "  timestamp INTEGER NOT NULL,"
        "  temp INTEGER NOT NULL,"
        "  pressure INTEGER NOT NULL,"
        "  flowRate INTEGER NOT NULL,"
        "  massFlow INTEGER NOT NULL,"
        "  volumeFlow INTEGER NOT NULL,"
        "  density INTEGER NOT NULL,"
        "  currentOfMotor INTEGER NOT NULL,"
        "  percentageOfValve INTEGER NOT NULL"
        ");";

    char* err = nullptr;
    if (sqlite3_exec(db, newSchema, nullptr, nullptr, &err) != SQLITE_OK) {
        fprintf(stderr, "Failed to create SQLite schema: %s\n", err ? err : "unknown error");
        sqlite3_free(err);
        sqlite3_close(db);
        return nullptr;
    }

    sqlite3_stmt* tableInfo = nullptr;
    if (sqlite3_prepare_v2(db, "PRAGMA table_info(sensor_rows);", -1, &tableInfo, nullptr) == SQLITE_OK) {
        bool needsMigration = false;
        while (sqlite3_step(tableInfo) == SQLITE_ROW) {
            const char* columnName = reinterpret_cast<const char*>(sqlite3_column_text(tableInfo, 1));
            if (columnName && (std::strcmp(columnName, "initial_time") == 0 ||
                              std::strcmp(columnName, "end_time") == 0 ||
                              std::strcmp(columnName, "row_index") == 0)) {
                needsMigration = true;
                break;
            }
        }
        sqlite3_finalize(tableInfo);

        if (needsMigration) {
            static const char* migrateSql =
                "CREATE TABLE sensor_rows_new ("
                "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
                "  stream_id INTEGER NOT NULL,"
                "  address TEXT NOT NULL,"
                "  row_count INTEGER NOT NULL DEFAULT 0,"
                "  received_begin_time INTEGER NOT NULL DEFAULT 0,"
                "  received_end_time INTEGER NOT NULL DEFAULT 0,"
                "  timestamp INTEGER NOT NULL,"
                "  temp INTEGER NOT NULL,"
                "  pressure INTEGER NOT NULL,"
                "  flowRate INTEGER NOT NULL,"
                "  massFlow INTEGER NOT NULL,"
                "  volumeFlow INTEGER NOT NULL,"
                "  density INTEGER NOT NULL,"
                "  currentOfMotor INTEGER NOT NULL,"
                "  percentageOfValve INTEGER NOT NULL"
                ");"
                "INSERT INTO sensor_rows_new(stream_id,address,row_count,received_begin_time,received_end_time,timestamp,temp,pressure,flowRate,massFlow,volumeFlow,density,currentOfMotor,percentageOfValve) "
                "SELECT stream_id,address,row_count,received_begin_time,received_end_time,timestamp,temp,pressure,flowRate,massFlow,volumeFlow,density,currentOfMotor,percentageOfValve FROM sensor_rows;"
                "DROP TABLE sensor_rows;"
                "ALTER TABLE sensor_rows_new RENAME TO sensor_rows;"
                "CREATE INDEX IF NOT EXISTS idx_sensor_rows_stream_time ON sensor_rows(stream_id, timestamp);"
                "CREATE INDEX IF NOT EXISTS idx_sensor_rows_timestamp ON sensor_rows(timestamp);";

            if (sqlite3_exec(db, migrateSql, nullptr, nullptr, &err) != SQLITE_OK) {
                fprintf(stderr, "Failed to migrate legacy sensor_rows schema: %s\n", err ? err : "unknown error");
                sqlite3_free(err);
                sqlite3_close(db);
                return nullptr;
            }
        } else {
            static const char* indexSql =
                "CREATE INDEX IF NOT EXISTS idx_sensor_rows_stream_time ON sensor_rows(stream_id, timestamp);"
                "CREATE INDEX IF NOT EXISTS idx_sensor_rows_timestamp ON sensor_rows(timestamp);";
            if (sqlite3_exec(db, indexSql, nullptr, nullptr, &err) != SQLITE_OK) {
                fprintf(stderr, "Failed to create SQLite indexes: %s\n", err ? err : "unknown error");
                sqlite3_free(err);
                sqlite3_close(db);
                return nullptr;
            }
        }
    }
    return db;
}

bool insertRows(sqlite3* db, uint8_t streamId, const std::string& address,
                int64_t receivedBeginTime, int64_t receivedEndTime,
                const std::vector<SensorRow>& rows) {
    if (rows.empty()) return true;

    static const char* sql =
        "INSERT INTO sensor_rows(stream_id,address,row_count,received_begin_time,"
        "received_end_time,timestamp,temp,pressure,flowRate,massFlow,volumeFlow,"
        "density,currentOfMotor,percentageOfValve) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?);";

    if (sqlite3_exec(db, "BEGIN IMMEDIATE;", nullptr, nullptr, nullptr) != SQLITE_OK) {
        fprintf(stderr, "SQLite BEGIN failed: %s\n", sqlite3_errmsg(db));
        return false;
    }

    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        fprintf(stderr, "SQLite prepare failed: %s\n", sqlite3_errmsg(db));
        sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);
        return false;
    }

    for (size_t i = 0; i < rows.size(); ++i) {
        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);

        sqlite3_bind_int(stmt, 1, streamId);
        sqlite3_bind_text(stmt, 2, address.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(stmt, 3, static_cast<int>(rows.size()));
        sqlite3_bind_int64(stmt, 4, receivedBeginTime);
        sqlite3_bind_int64(stmt, 5, receivedEndTime);
        sqlite3_bind_int64(stmt, 6, rows[i].timestamp);
        sqlite3_bind_int64(stmt, 7, rows[i].temp);
        sqlite3_bind_int64(stmt, 8, rows[i].pressure);
        sqlite3_bind_int64(stmt, 9, rows[i].flowRate);
        sqlite3_bind_int64(stmt, 10, rows[i].massFlow);
        sqlite3_bind_int64(stmt, 11, rows[i].volumeFlow);
        sqlite3_bind_int64(stmt, 12, rows[i].density);
        sqlite3_bind_int64(stmt, 13, rows[i].currentOfMotor);
        sqlite3_bind_int64(stmt, 14, rows[i].percentageOfValve);

        if (sqlite3_step(stmt) != SQLITE_DONE) {
            fprintf(stderr, "SQLite row insert failed: %s\n", sqlite3_errmsg(db));
            sqlite3_finalize(stmt);
            sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);
            return false;
        }
    }

    sqlite3_finalize(stmt);
    if (sqlite3_exec(db, "COMMIT;", nullptr, nullptr, nullptr) != SQLITE_OK) {
        fprintf(stderr, "SQLite COMMIT failed: %s\n", sqlite3_errmsg(db));
        sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);
        return false;
    }
    return true;
}

class StreamWriter {
public:
    explicit StreamWriter(const std::string& brokers, const std::string& dataDir) {
        dbPath_ = dataDir + "\\phonepipe.sqlite3";
        db_ = openDb(dbPath_);

        std::string errstr;
        std::unique_ptr<RdKafka::Conf> conf(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
        conf->set("bootstrap.servers", brokers, errstr);
        conf->set("group.id", "phonepipe-db-writer", errstr);
        conf->set("enable.auto.commit", "true", errstr);
        conf->set("auto.offset.reset", "earliest", errstr);
        conf->set("allow.auto.create.topics", "true", errstr);

        consumer_.reset(RdKafka::KafkaConsumer::create(conf.get(), errstr));
        if (!consumer_) {
            fprintf(stderr, "Failed to create Kafka consumer: %s\n", errstr.c_str());
            return;
        }

        RdKafka::ErrorCode err = consumer_->subscribe({"phonepipe.low", "phonepipe.high"});
        if (err != RdKafka::ERR_NO_ERROR) {
            fprintf(stderr, "Subscribe failed: %s\n", RdKafka::err2str(err).c_str());
            return;
        }
    }

    ~StreamWriter() {
        if (consumer_) {
            consumer_->close();
        }
        if (db_) sqlite3_close(db_);
    }

    bool ok() const { return consumer_ && db_; }

    void run() {
        while (true) {
            std::unique_ptr<RdKafka::Message> msg(consumer_->consume(1000));
            switch (msg->err()) {
                case RdKafka::ERR__TIMED_OUT:
                    continue;
                case RdKafka::ERR_NO_ERROR:
                    handleMessage(static_cast<const uint8_t*>(msg->payload()), msg->len());
                    continue;
                case RdKafka::ERR__PARTITION_EOF:
                    continue;
                default:
                    fprintf(stderr, "Kafka consume error: %s\n", msg->errstr().c_str());
                    continue;
            }
        }
    }

private:
    void handleMessage(const uint8_t* raw, size_t len) {
        if (len < kHeaderSize) {
            fprintf(stderr, "short Kafka message (%zu bytes)\n", len);
            return;
        }

        const PacketHeader hdr = decodeHeader(raw);
        const uint8_t* payloadPtr = raw + kHeaderSize;
        const size_t wireSize = kHeaderSize + hdr.payloadLen;
        if (len != wireSize && len != wireSize + kLegacyReceivedTimeSize &&
            len != wireSize + kReceivedTimeSize) {
            fprintf(stderr, "packet length mismatch: header says %u, got %zu\n",
                    hdr.payloadLen, len - kHeaderSize);
            return;
        }

        const int64_t receivedBeginTime = len == wireSize + kReceivedTimeSize
            ? getReceivedBeginTimeMs(raw, wireSize)
            : (len == wireSize + kLegacyReceivedTimeSize
                ? getReceivedBeginTimeMs(raw, wireSize)
                : 0);
        const int64_t receivedEndTime = len == wireSize + kReceivedTimeSize
            ? getReceivedEndTimeMs(raw, wireSize)
            : receivedBeginTime;

        const std::vector<SensorRow> rows = decodePayload(hdr, payloadPtr, hdr.payloadLen);
        if (rows.empty()) {
            fprintf(stderr, "failed to decode payload for stream %u\n", hdr.streamId);
            return;
        }

        if (!insertRows(db_, hdr.streamId, streamName(hdr.streamId),
                        receivedBeginTime, receivedEndTime, rows)) {
            fprintf(stderr, "DB insert failed for stream %u (%s)\n",
                    hdr.streamId, streamName(hdr.streamId));
            return;
        }

        printf("[%s:%u] decoded %zu rows, first_ts=%lld, last_ts=%lld\n",
               streamName(hdr.streamId), hdr.streamId, rows.size(),
               static_cast<long long>(rows.front().timestamp),
               static_cast<long long>(rows.back().timestamp));
    }

    std::string dbPath_;
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
    CreateDirectoryA(dataDir.c_str(), nullptr);

    StreamWriter writer(brokers, dataDir);
    if (!writer.ok()) {
        fprintf(stderr, "Failed to initialize Kafka stream writer\n");
        return 1;
    }
    writer.run();
    return 0;
}
