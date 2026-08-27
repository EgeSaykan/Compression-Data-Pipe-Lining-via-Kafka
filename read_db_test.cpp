#include "read_db.h"

#include <cassert>
#include <cstdio>
#include <fstream>
#include <sqlite3.h>

int main() {
    const char* databasePath = "read_db_test.sqlite3";
    const char* binaryPath = "read_db_test.bin";
    std::remove(databasePath);
    std::remove(binaryPath);

    {
        std::ofstream file(binaryPath, std::ios::binary);
        const char bytes[] = "abcdef";
        file.write(bytes, sizeof(bytes) - 1);
    }
    sqlite3* db = nullptr;
    assert(sqlite3_open(databasePath, &db) == SQLITE_OK);
    assert(sqlite3_exec(db,
        "CREATE TABLE batches (id INTEGER PRIMARY KEY AUTOINCREMENT, stream_id INTEGER NOT NULL, "
        "address TEXT NOT NULL, begin_index INTEGER NOT NULL, end_index INTEGER NOT NULL, "
        "initial_time INTEGER NOT NULL, end_time INTEGER NOT NULL, "
        "received_begin_time INTEGER NOT NULL DEFAULT 0, received_end_time INTEGER NOT NULL DEFAULT 0);"
        "INSERT INTO batches(stream_id,address,begin_index,end_index,initial_time,end_time) "
        "VALUES(0,'green',0,3,100,102);"
        "INSERT INTO batches(stream_id,address,begin_index,end_index,initial_time,end_time) "
        "VALUES(1,'pink',3,6,200,202);", nullptr, nullptr, nullptr) == SQLITE_OK);
    sqlite3_close(db);

    tabletpipe::ReadDb reader(databasePath, binaryPath);
    auto first = reader.readSince(0);
    assert(first.ok() && first.records.size() == 2);
    assert(first.records[0].id == 1 && first.records[0].compressedBytes[0] == 'a');
    auto second = reader.readRange({tabletpipe::RangeKey::Timestamp, 1, 150, 250});
    assert(second.ok() && second.records.size() == 1 && second.records[0].id == 2);
    auto empty = reader.readSince(2);
    assert(empty.ok() && empty.records.empty());

    std::remove(databasePath);
    std::remove(binaryPath);
    return 0;
}
