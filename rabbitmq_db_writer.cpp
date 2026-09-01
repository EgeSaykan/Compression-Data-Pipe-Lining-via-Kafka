#include <windows.h>

#ifndef AMQP_STATIC
#define AMQP_STATIC
#endif
#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "wire_protocol.h"
#include "gzip_codec.h"
#include "openzl_codec.h"
#include <sqlite3.h>

using namespace phonepipe;

namespace {

const char* envOr(const char* name, const char* fallback) {
    const char* value = std::getenv(name);
    return value && *value ? value : fallback;
}

bool rabbitOk(amqp_rpc_reply_t reply, const char* operation) {
    if (reply.reply_type == AMQP_RESPONSE_NORMAL) return true;
    fprintf(stderr, "RabbitMQ %s failed (reply type %d)\n", operation, reply.reply_type);
    return false;
}

sqlite3* openDb(const std::string& path) {
    sqlite3* db = nullptr;
    if (sqlite3_open(path.c_str(), &db) != SQLITE_OK) return nullptr;
    sqlite3_busy_timeout(db, 5000);
    sqlite3_exec(db, "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;", nullptr, nullptr, nullptr);
    const char* schema =
        "CREATE TABLE IF NOT EXISTS batches ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, stream_id INTEGER NOT NULL,"
        "address TEXT NOT NULL, begin_index INTEGER NOT NULL, end_index INTEGER NOT NULL,"
        "initial_time INTEGER NOT NULL, end_time INTEGER NOT NULL,"
        "row_count INTEGER NOT NULL DEFAULT 0,"
        "received_time INTEGER NOT NULL DEFAULT 0,"
        "received_begin_time INTEGER NOT NULL DEFAULT 0,"
        "received_end_time INTEGER NOT NULL DEFAULT 0);"
        "CREATE INDEX IF NOT EXISTS idx_batches_stream_time ON batches(stream_id, initial_time);";
    char* error = nullptr;
    if (sqlite3_exec(db, schema, nullptr, nullptr, &error) != SQLITE_OK) {
        fprintf(stderr, "SQLite schema failed: %s\n", error);
        sqlite3_free(error);
        sqlite3_close(db);
        return nullptr;
    }
    sqlite3_exec(db, "ALTER TABLE batches ADD COLUMN received_time INTEGER NOT NULL DEFAULT 0;",
                 nullptr, nullptr, nullptr);
    sqlite3_exec(db, "ALTER TABLE batches ADD COLUMN received_begin_time INTEGER NOT NULL DEFAULT 0;",
                 nullptr, nullptr, nullptr);
    sqlite3_exec(db, "ALTER TABLE batches ADD COLUMN received_end_time INTEGER NOT NULL DEFAULT 0;",
                 nullptr, nullptr, nullptr);
    sqlite3_exec(db, "ALTER TABLE batches ADD COLUMN row_count INTEGER NOT NULL DEFAULT 0;",
                 nullptr, nullptr, nullptr);
    return db;
}

bool insertBatch(sqlite3* db, uint8_t streamId, const char* name,
                 long begin, long end, int64_t initialTime, int64_t endTime,
                 int64_t receivedBeginTime, int64_t receivedEndTime, uint32_t rowCount) {
    sqlite3_stmt* statement = nullptr;
    const char* sql =
        "INSERT INTO batches(stream_id,address,begin_index,end_index,initial_time,end_time,"
        "received_time,received_begin_time,received_end_time,row_count) VALUES(?,?,?,?,?,?,?,?,?,?)";
    if (sqlite3_prepare_v2(db, sql, -1, &statement, nullptr) != SQLITE_OK) return false;
    sqlite3_bind_int(statement, 1, streamId);
    sqlite3_bind_text(statement, 2, name, -1, SQLITE_STATIC);
    sqlite3_bind_int64(statement, 3, begin);
    sqlite3_bind_int64(statement, 4, end);
    sqlite3_bind_int64(statement, 5, initialTime);
    sqlite3_bind_int64(statement, 6, endTime);
    sqlite3_bind_int64(statement, 7, receivedEndTime);
    sqlite3_bind_int64(statement, 8, receivedBeginTime);
    sqlite3_bind_int64(statement, 9, receivedEndTime);
    sqlite3_bind_int(statement, 10, static_cast<int>(rowCount));
    const bool ok = sqlite3_step(statement) == SQLITE_DONE;
    sqlite3_finalize(statement);
    return ok;
}

class RabbitPriorityConsumer {
public:
    RabbitPriorityConsumer() {
        CreateDirectoryA("data", nullptr);
        dataFile_ = fopen("data\\phonepipe.bin", "ab+");
        db_ = openDb("data\\phonepipe.sqlite3");
        connection_ = amqp_new_connection();
        socket_ = amqp_tcp_socket_new(connection_);
        if (!socket_ || amqp_socket_open(socket_, envOr("RABBITMQ_HOST", "localhost"),
                                         std::atoi(envOr("RABBITMQ_PORT", "5672")))) return;
        if (!rabbitOk(amqp_login(connection_, envOr("RABBITMQ_VHOST", "/"), 0, 131072, 0,
                                 AMQP_SASL_METHOD_PLAIN, envOr("RABBITMQ_USER", "socar"),
                                 envOr("RABBITMQ_PASSWORD", "socar-dev-password")), "login")) return;
        amqp_channel_open(connection_, channel_);
        if (!rabbitOk(amqp_get_rpc_reply(connection_), "channel open")) return;

        const std::string exchange = envOr("RABBITMQ_EXCHANGE", "phonepipe");
        amqp_exchange_declare(connection_, channel_, amqp_cstring_bytes(exchange.c_str()),
                              amqp_cstring_bytes("direct"), 0, 1, 0, 0, amqp_empty_table);
        if (!rabbitOk(amqp_get_rpc_reply(connection_), "exchange declare")) return;

        amqp_table_entry_t priorityEntry{};
        priorityEntry.key = amqp_cstring_bytes("x-max-priority");
        priorityEntry.value.kind = AMQP_FIELD_KIND_I32;
        priorityEntry.value.value.i32 = 10;
        amqp_table_t queueArguments{};
        queueArguments.num_entries = 1;
        queueArguments.entries = &priorityEntry;

        queue_ = "phonepipe-db";
        amqp_queue_declare(connection_, channel_, amqp_cstring_bytes(queue_.c_str()),
                           0, 1, 0, 0, queueArguments);
        if (!rabbitOk(amqp_get_rpc_reply(connection_), "queue declare")) return;
        for (const char* routingKey : {"high", "low"}) {
            amqp_queue_bind(connection_, channel_, amqp_cstring_bytes(queue_.c_str()),
                            amqp_cstring_bytes(exchange.c_str()), amqp_cstring_bytes(routingKey),
                            amqp_empty_table);
            if (!rabbitOk(amqp_get_rpc_reply(connection_), "queue bind")) return;
        }
        amqp_basic_qos(connection_, channel_, 0, 1, 0);
        amqp_basic_consume(connection_, channel_, amqp_cstring_bytes(queue_.c_str()),
                           amqp_empty_bytes, 0, 0, 0, amqp_empty_table);
        ready_ = rabbitOk(amqp_get_rpc_reply(connection_), "consume setup");
    }

    ~RabbitPriorityConsumer() {
        if (connection_) {
            if (ready_) amqp_channel_close(connection_, channel_, AMQP_REPLY_SUCCESS);
            amqp_connection_close(connection_, AMQP_REPLY_SUCCESS);
            amqp_destroy_connection(connection_);
        }
        if (dataFile_) fclose(dataFile_);
        if (db_) sqlite3_close(db_);
    }

    bool ok() const { return ready_ && dataFile_ && db_; }

    void run() {
        while (true) {
            amqp_envelope_t envelope;
            const amqp_rpc_reply_t reply = amqp_consume_message(connection_, &envelope, nullptr, 1000);
            if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
                const bool stored = handle(static_cast<const uint8_t*>(envelope.message.body.bytes),
                                           envelope.message.body.len);
                if (stored) amqp_basic_ack(connection_, channel_, envelope.delivery_tag, 0);
                else amqp_basic_reject(connection_, channel_, envelope.delivery_tag, 0);
                amqp_destroy_envelope(&envelope);
            } else if (reply.reply_type != AMQP_RESPONSE_LIBRARY_EXCEPTION) {
                fprintf(stderr, "RabbitMQ consume error\n");
            }
        }
    }

private:
    bool handle(const uint8_t* raw, size_t length) {
        if (length < kHeaderSize) return false;
        const PacketHeader header = decodeHeader(raw);
        const size_t wireSize = kHeaderSize + header.payloadLen;
        if (length != wireSize && length != wireSize + kLegacyReceivedTimeSize &&
            length != wireSize + kReceivedTimeSize) return false;
        const int64_t receivedBeginTime = length == wireSize + kReceivedTimeSize
            ? getReceivedBeginTimeMs(raw, wireSize)
            : (length == wireSize + kLegacyReceivedTimeSize
                ? getReceivedBeginTimeMs(raw, wireSize) : 0);
        const int64_t receivedEndTime = length == wireSize + kReceivedTimeSize
            ? getReceivedEndTimeMs(raw, wireSize) : receivedBeginTime;

        const uint8_t* payloadPtr = raw + kHeaderSize;
        std::vector<uint8_t> compressed;
        if (header.flag == CompressionFlag::OPENZL) {
            // Already normalized on the receiver; keep the payload as-is.
            compressed.assign(payloadPtr, payloadPtr + header.payloadLen);
        } else if (header.flag == CompressionFlag::RAW) {
            // Raw delta-encoded columnar payload: compress to OpenZL before writing.
            compressed = openzlCompressDeltaEncoded(payloadPtr, header.payloadLen,
                                                     static_cast<int>(header.rowCount));
        } else if (header.flag == CompressionFlag::GZIP) {
            // GZIP packets carry the plain (non-delta-encoded) columnar buffer.
            std::vector<uint8_t> decompressed = gzipDecompress(payloadPtr, header.payloadLen);
            if (decompressed.empty()) {
                fprintf(stderr, "gzip decompress failed for stream %u\n", header.streamId);
                return false;
            }
            compressed = openzlCompressFresh(decompressed.data(), decompressed.size(),
                                              static_cast<int>(header.rowCount));
        } else {
            fprintf(stderr, "unknown compression flag %u for stream %u\n",
                    static_cast<unsigned>(header.flag), header.streamId);
            return false;
        }
        if (compressed.empty()) return false;
        if (fseek(dataFile_, 0, SEEK_END) != 0) return false;
        const long begin = ftell(dataFile_);
        if (begin < 0 || fwrite(compressed.data(), 1, compressed.size(), dataFile_) != compressed.size()) return false;
        fflush(dataFile_);
        const long end = begin + static_cast<long>(compressed.size());
        if (!insertBatch(db_, header.streamId, streamName(header.streamId), begin, end,
                         header.initialTimeMs, header.endTimeMs,
                         receivedBeginTime, receivedEndTime, header.rowCount)) return false;
        printf("[%s:%u] wrote %zu bytes at [%ld, %ld)\n",
               streamName(header.streamId), header.streamId, compressed.size(), begin, end);
        return true;
    }

    std::string queue_;
    FILE* dataFile_ = nullptr;
    sqlite3* db_ = nullptr;
    amqp_connection_state_t connection_ = nullptr;
    amqp_socket_t* socket_ = nullptr;
    amqp_channel_t channel_ = 1;
    bool ready_ = false;
};

} // namespace

int main() {
    RabbitPriorityConsumer consumer;
    if (!consumer.ok()) {
        fprintf(stderr, "Failed to initialize RabbitMQ stream consumer\n");
        return 1;
    }
    consumer.run();
    return 0;
}