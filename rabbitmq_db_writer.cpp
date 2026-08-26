// RabbitMQ -> OpenZL-compressed per-stream files + SQLite metadata.
// Build dependencies: rabbitmq-c, sqlite3, and OpenZL.
//
// Both streams use one RabbitMQ priority queue. Pink has higher priority and
// is therefore selected first whenever pink and green are waiting together.

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
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "wire_protocol.h"
#include <sqlite3.h>
#include "openzl/zl_compress.h"
#include "openzl/zl_compressor.h"
#include "openzl/zl_errors.h"

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

std::vector<uint8_t> compressColumns(const std::vector<uint8_t>& rawPayload, uint32_t rowCount) {
    const size_t columnBytes = static_cast<size_t>(rowCount) * kColWidth;
    const size_t expectedSize = static_cast<size_t>(kFieldCount) * columnBytes;
    if (rawPayload.size() != expectedSize || expectedSize == 0) return {};

    ZL_Compressor* compressor = ZL_Compressor_create();
    if (!compressor) return {};
    ZL_Report report = ZL_Compressor_setParameter(
        compressor, ZL_CParam_formatVersion, ZL_MAX_FORMAT_VERSION);
    if (!ZL_isError(report)) {
        report = ZL_Compressor_selectStartingGraphID(compressor, ZL_GRAPH_COMPRESS_GENERIC);
    }
    ZL_CCtx* context = ZL_CCtx_create();
    if (!context || ZL_isError(report)) {
        if (context) ZL_CCtx_free(context);
        ZL_Compressor_free(compressor);
        return {};
    }
    report = ZL_CCtx_refCompressor(context, compressor);

    std::vector<int64_t> alignedData(expectedSize / sizeof(int64_t));
    std::memcpy(alignedData.data(), rawPayload.data(), expectedSize);
    std::vector<ZL_TypedRef*> inputs(kFieldCount, nullptr);
    bool inputsReady = !ZL_isError(report);
    for (int column = 0; column < kFieldCount && inputsReady; ++column) {
        inputs[column] = ZL_TypedRef_createNumeric(
            reinterpret_cast<uint8_t*>(alignedData.data()) + column * columnBytes,
            sizeof(int64_t), rowCount);
        if (!inputs[column]) inputsReady = false;
    }

    std::vector<const ZL_TypedRef*> typedInputs(inputs.begin(), inputs.end());
    std::vector<uint8_t> output(expectedSize + expectedSize / 2 + 4096);
    if (inputsReady && !ZL_isError(report)) {
        report = ZL_CCtx_compressMultiTypedRef(
            context, output.data(), output.size(), typedInputs.data(), typedInputs.size());
    }

    std::vector<uint8_t> result;
    if (!ZL_isError(report)) result.assign(output.begin(), output.begin() + ZL_validResult(report));
    for (ZL_TypedRef* input : inputs) {
        if (input) ZL_TypedRef_free(input);
    }
    ZL_CCtx_free(context);
    ZL_Compressor_free(compressor);
    return result;
}

sqlite3* openDb(const std::string& path) {
    sqlite3* db = nullptr;
    if (sqlite3_open(path.c_str(), &db) != SQLITE_OK) return nullptr;
    sqlite3_busy_timeout(db, 5000);
    sqlite3_exec(db, "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;", nullptr, nullptr, nullptr);
    const char* schema =
        "CREATE TABLE IF NOT EXISTS batches ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, address TEXT NOT NULL,"
        "begin_index INTEGER NOT NULL, end_index INTEGER NOT NULL,"
        "initial_time INTEGER NOT NULL, end_time INTEGER NOT NULL);"
        "CREATE INDEX IF NOT EXISTS idx_batches_address_time ON batches(address, initial_time);";
    char* error = nullptr;
    if (sqlite3_exec(db, schema, nullptr, nullptr, &error) != SQLITE_OK) {
        fprintf(stderr, "SQLite schema failed: %s\n", error);
        sqlite3_free(error);
        sqlite3_close(db);
        return nullptr;
    }
    return db;
}

bool insertBatch(sqlite3* db, const std::string& name, int64_t begin, int64_t end,
                 int64_t initialTime, int64_t endTime) {
    sqlite3_stmt* statement = nullptr;
    const char* sql = "INSERT INTO batches(address,begin_index,end_index,initial_time,end_time) VALUES(?,?,?,?,?)";
    if (sqlite3_prepare_v2(db, sql, -1, &statement, nullptr) != SQLITE_OK) return false;
    sqlite3_bind_text(statement, 1, name.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(statement, 2, begin);
    sqlite3_bind_int64(statement, 3, end);
    sqlite3_bind_int64(statement, 4, initialTime);
    sqlite3_bind_int64(statement, 5, endTime);
    const bool ok = sqlite3_step(statement) == SQLITE_DONE;
    sqlite3_finalize(statement);
    return ok;
}

class RabbitPriorityConsumer {
public:
    RabbitPriorityConsumer() {
        CreateDirectoryA("data", nullptr);
        pinkFile_ = fopen("data\\pink.bin", "ab+");
        greenFile_ = fopen("data\\green.bin", "ab+");
        pinkDb_ = openDb("data\\pink.sqlite3");
        greenDb_ = openDb("data\\green.sqlite3");
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
        for (const char* routingKey : {"pink", "green"}) {
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
        if (pinkFile_) fclose(pinkFile_);
        if (greenFile_) fclose(greenFile_);
        if (pinkDb_) sqlite3_close(pinkDb_);
        if (greenDb_) sqlite3_close(greenDb_);
    }

    bool ok() const {
        return ready_ && pinkFile_ && greenFile_ && pinkDb_ && greenDb_;
    }

    void run() {
        while (true) {
            amqp_envelope_t envelope;
            const amqp_rpc_reply_t reply = amqp_consume_message(connection_, &envelope, nullptr, 1000);
            if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
                const bool stored = handle(
                    static_cast<const uint8_t*>(envelope.message.body.bytes),
                    envelope.message.body.len);
                if (stored) {
                    amqp_basic_ack(connection_, channel_, envelope.delivery_tag, 0);
                } else {
                    // Do not redeliver a malformed or permanently unwritable batch.
                    amqp_basic_reject(connection_, channel_, envelope.delivery_tag, 0);
                }
                amqp_destroy_envelope(&envelope);
            } else if (reply.reply_type != AMQP_RESPONSE_LIBRARY_EXCEPTION) {
                fprintf(stderr, "[%s] RabbitMQ consume error\n", queue_.c_str());
            }
        }
    }

private:
    bool handle(const uint8_t* raw, size_t length) {
        if (length < kHeaderSize) return false;
        const PacketHeader header = decodeHeader(raw);
        if (header.streamId != STREAM_GREEN && header.streamId != STREAM_PINK) return false;
        if (length - kHeaderSize != header.payloadLen) return false;

        const char* name = streamName(header.streamId);
        FILE* file = header.streamId == STREAM_PINK ? pinkFile_ : greenFile_;
        sqlite3* db = header.streamId == STREAM_PINK ? pinkDb_ : greenDb_;

        std::vector<uint8_t> payload(raw + kHeaderSize, raw + length);
        std::vector<uint8_t> compressed = header.compressed
            ? std::move(payload) : compressColumns(payload, header.rowCount);
         if (compressed.empty()) return false;

           if (fseek(file, 0, SEEK_END) != 0) return false;
           const long begin = ftell(file);
           if (begin < 0 || fwrite(compressed.data(), 1, compressed.size(), file) != compressed.size()) return false;
           fflush(file);
        const long end = begin + static_cast<long>(compressed.size());
           if (!insertBatch(db, name, begin, end, header.initialTimeMs, header.endTimeMs)) return false;
         printf("[%s] wrote %zu bytes at [%ld, %ld), t=[%lld..%lld]\n",
               name, compressed.size(), begin, end,
             static_cast<long long>(header.initialTimeMs),
             static_cast<long long>(header.endTimeMs));
         return true;
    }

    std::string queue_;
    FILE* pinkFile_ = nullptr;
    FILE* greenFile_ = nullptr;
    sqlite3* pinkDb_ = nullptr;
    sqlite3* greenDb_ = nullptr;
    amqp_connection_state_t connection_ = nullptr;
    amqp_socket_t* socket_ = nullptr;
    amqp_channel_t channel_ = 1;
    bool ready_ = false;
};

} // namespace

int main() {
    RabbitPriorityConsumer consumer;
    if (!consumer.ok()) {
        fprintf(stderr, "Failed to initialize RabbitMQ stream consumers\n");
        return 1;
    }

    consumer.run();
    return 0;
}