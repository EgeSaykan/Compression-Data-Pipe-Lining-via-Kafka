#include "read_db.h"

#include <cstdio>
#include <fstream>
#include <limits>
#include <utility>
#include <sqlite3.h>

namespace tabletpipe {
namespace {

class DbHandle {
public:
    explicit DbHandle(const std::string& path) {
        if (sqlite3_open_v2(path.c_str(), &db_, SQLITE_OPEN_READONLY, nullptr) != SQLITE_OK) {
            if (db_) error_ = sqlite3_errmsg(db_);
            else error_ = "could not open database";
            return;
        }
        sqlite3_busy_timeout(db_, 5000);
    }

    ~DbHandle() { if (db_) sqlite3_close(db_); }
    DbHandle(const DbHandle&) = delete;
    DbHandle& operator=(const DbHandle&) = delete;
    sqlite3* get() const { return db_; }
    const std::string& error() const { return error_; }

private:
    sqlite3* db_ = nullptr;
    std::string error_;
};

ReadResult failure(const std::string& message) {
    ReadResult result;
    result.error = message;
    return result;
}

bool readBlob(const std::string& path, int64_t begin, int64_t end,
              std::vector<uint8_t>& output, std::string& error) {
    if (begin < 0 || end < begin) {
        error = "invalid binary range";
        return false;
    }
    const uint64_t length = static_cast<uint64_t>(end - begin);
    if (length > std::numeric_limits<size_t>::max()) {
        error = "binary range is too large";
        return false;
    }

    std::ifstream file(path, std::ios::binary);
    if (!file) {
        error = "could not open binary data file: " + path;
        return false;
    }
    file.seekg(begin, std::ios::beg);
    if (!file) {
        error = "could not seek in binary data file";
        return false;
    }
    output.resize(static_cast<size_t>(length));
    if (length != 0 && !file.read(reinterpret_cast<char*>(output.data()),
                                  static_cast<std::streamsize>(length))) {
        error = "binary data range is truncated";
        output.clear();
        return false;
    }
    return true;
}

} // namespace

ReadDb::ReadDb(std::string databasePath, std::string binaryPath)
    : databasePath_(std::move(databasePath)), binaryPath_(std::move(binaryPath)) {}

ReadResult ReadDb::readRange(const RecordRange& range) const {
    if (range.start > range.end) return failure("range start is after range end");
    if (range.key != RangeKey::Id && range.key != RangeKey::Timestamp) {
        return failure("unsupported range key");
    }

    const char* column = range.key == RangeKey::Id ? "id" : "timestamp";
    const std::string sql =
        "SELECT id, stream_id, address, row_count, received_begin_time, received_end_time, timestamp "
        "FROM sensor_rows WHERE " + std::string(column) + " >= ? AND " + std::string(column) + " <= ?" +
        (range.streamId != 0xff ? " AND stream_id = ?" : "") + " ORDER BY id ASC;";
    std::vector<int64_t> values{range.start, range.end};
    if (range.streamId != 0xff) values.push_back(range.streamId);
    return readQuery(sql, values);
}

ReadResult ReadDb::readSince(int64_t cursorId, uint8_t streamId) const {
    if (cursorId < 0) return failure("cursor id cannot be negative");
    const std::string sql =
        "SELECT id, stream_id, address, row_count, received_begin_time, received_end_time, timestamp "
        "FROM sensor_rows WHERE id > ?" +
        std::string(streamId != 0xff ? " AND stream_id = ?" : "") + " ORDER BY id ASC;";
    std::vector<int64_t> values{cursorId};
    if (streamId != 0xff) values.push_back(streamId);
    return readQuery(sql, values);
}

bool ReadDb::maxId(int64_t& id, std::string& error) const {
    DbHandle db(databasePath_);
    if (!db.get()) { error = db.error(); return false; }
    sqlite3_stmt* statement = nullptr;
    if (sqlite3_prepare_v2(db.get(), "SELECT COALESCE(MAX(id), 0) FROM sensor_rows;", -1,
                           &statement, nullptr) != SQLITE_OK) {
        if (sqlite3_prepare_v2(db.get(), "SELECT COALESCE(MAX(id), 0) FROM batches;", -1,
                               &statement, nullptr) != SQLITE_OK) {
            error = sqlite3_errmsg(db.get());
            return false;
        }
    }
    const bool ok = sqlite3_step(statement) == SQLITE_ROW;
    if (ok) id = sqlite3_column_int64(statement, 0);
    else error = sqlite3_errmsg(db.get());
    sqlite3_finalize(statement);
    return ok;
}

ReadResult ReadDb::readQuery(const std::string& sql, const std::vector<int64_t>& values) const {
    DbHandle db(databasePath_);
    if (!db.get()) return failure(db.error());

    sqlite3_stmt* statement = nullptr;
    if (sqlite3_prepare_v2(db.get(), sql.c_str(), -1, &statement, nullptr) != SQLITE_OK) {
        return failure(sqlite3_errmsg(db.get()));
    }
    for (size_t index = 0; index < values.size(); ++index) {
        sqlite3_bind_int64(statement, static_cast<int>(index + 1), values[index]);
    }

    ReadResult result;
    while (true) {
        const int status = sqlite3_step(statement);
        if (status == SQLITE_DONE) break;
        if (status != SQLITE_ROW) {
            result.error = sqlite3_errmsg(db.get());
            break;
        }

        Record record;
        record.id = sqlite3_column_int64(statement, 0);
        record.streamId = static_cast<uint8_t>(sqlite3_column_int(statement, 1));
        const unsigned char* address = sqlite3_column_text(statement, 2);
        record.address = address ? reinterpret_cast<const char*>(address) : "";
        record.rowCount = sqlite3_column_int64(statement, 3);
        record.receivedBeginTime = sqlite3_column_int64(statement, 4);
        record.receivedEndTime = sqlite3_column_int64(statement, 5);
        record.timestamp = sqlite3_column_int64(statement, 6);
        result.records.push_back(std::move(record));
    }
    sqlite3_finalize(statement);
    if (!result.error.empty()) result.records.clear();
    return result;
}

} // namespace tabletpipe
