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

const char* envOr(const char* name, const char* fallback) {
    const char* value = std::getenv(name);
    return value && *value ? value : fallback;
}

bool rabbitOk(amqp_rpc_reply_t reply, const char* operation) {
    if (reply.reply_type == AMQP_RESPONSE_NORMAL) return true;
    fprintf(stderr, "RabbitMQ %s failed (reply type %d)\n", operation, reply.reply_type);
    return false;
}

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
    const size_t rowCountSize = static_cast<size_t>(rowCount);
    for (int c = 0; c < kFieldCount; ++c) {
        int64_t* col = rawData.data() + static_cast<size_t>(c) * rowCountSize;
        deltaDecodeInPlace(col, static_cast<int>(rowCount));
    }

    std::vector<SensorRow> rows(rowCount);
    const int64_t* timestamps = rawData.data();
    const int64_t* temp = timestamps + rowCountSize;
    const int64_t* pressure = temp + rowCountSize;
    const int64_t* flowRate = pressure + rowCountSize;
    const int64_t* massFlow = flowRate + rowCountSize;
    const int64_t* volumeFlow = massFlow + rowCountSize;
    const int64_t* density = volumeFlow + rowCountSize;
    const int64_t* currentOfMotor = density + rowCountSize;
    const int64_t* percentageOfValve = currentOfMotor + rowCountSize;

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

    const size_t rowCountSize = static_cast<size_t>(rowCount);
    const size_t colBytes = rowCountSize * kColWidth;
    std::vector<int64_t> flat(static_cast<size_t>(kFieldCount) * rowCountSize);
    size_t offset = 0;
    for (int c = 0; c < kFieldCount; ++c) {
        const auto* src = reinterpret_cast<const int64_t*>(col(c));
        std::memcpy(flat.data() + offset, src, colBytes);
        int64_t* dest = flat.data() + offset;
        deltaDecodeInPlace(dest, static_cast<int>(rowCount));
        offset += rowCountSize;
    }

    std::vector<SensorRow> rows(rowCount);
    const int64_t* timestamps = flat.data();
    const int64_t* temp = timestamps + rowCountSize;
    const int64_t* pressure = temp + rowCountSize;
    const int64_t* flowRate = pressure + rowCountSize;
    const int64_t* massFlow = flowRate + rowCountSize;
    const int64_t* volumeFlow = massFlow + rowCountSize;
    const int64_t* density = volumeFlow + rowCountSize;
    const int64_t* currentOfMotor = density + rowCountSize;
    const int64_t* percentageOfValve = currentOfMotor + rowCountSize;

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

    char* error = nullptr;
    if (sqlite3_exec(db, newSchema, nullptr, nullptr, &error) != SQLITE_OK) {
        fprintf(stderr, "SQLite schema failed: %s\n", error ? error : "unknown error");
        sqlite3_free(error);
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

            if (sqlite3_exec(db, migrateSql, nullptr, nullptr, &error) != SQLITE_OK) {
                fprintf(stderr, "Failed to migrate legacy sensor_rows schema: %s\n", error ? error : "unknown error");
                sqlite3_free(error);
                sqlite3_close(db);
                return nullptr;
            }
        } else {
            static const char* indexSql =
                "CREATE INDEX IF NOT EXISTS idx_sensor_rows_stream_time ON sensor_rows(stream_id, timestamp);"
                "CREATE INDEX IF NOT EXISTS idx_sensor_rows_timestamp ON sensor_rows(timestamp);";
            if (sqlite3_exec(db, indexSql, nullptr, nullptr, &error) != SQLITE_OK) {
                fprintf(stderr, "Failed to create SQLite indexes: %s\n", error ? error : "unknown error");
                sqlite3_free(error);
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

class RabbitPriorityConsumer {
public:
    RabbitPriorityConsumer() {
        CreateDirectoryA("data", nullptr);
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
        if (db_) sqlite3_close(db_);
    }

    bool ok() const { return ready_ && db_; }

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
                ? getReceivedBeginTimeMs(raw, wireSize)
                : 0);
        const int64_t receivedEndTime = length == wireSize + kReceivedTimeSize
            ? getReceivedEndTimeMs(raw, wireSize)
            : receivedBeginTime;

        const std::vector<SensorRow> rows = decodePayload(header, raw + kHeaderSize, header.payloadLen);
        if (rows.empty()) {
            fprintf(stderr, "failed to decode payload for stream %u\n", header.streamId);
            return false;
        }

        if (!insertRows(db_, header.streamId, streamName(header.streamId),
                        receivedBeginTime, receivedEndTime, rows)) {
            fprintf(stderr, "DB insert failed for stream %u (%s)\n",
                    header.streamId, streamName(header.streamId));
            return false;
        }

        printf("[%s:%u] decoded %zu rows, first_ts=%lld, last_ts=%lld\n",
               streamName(header.streamId), header.streamId, rows.size(),
               static_cast<long long>(rows.front().timestamp),
               static_cast<long long>(rows.back().timestamp));
        return true;
    }

    std::string queue_;
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
