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

    sqlite3* db = nullptr;
    assert(sqlite3_open(databasePath, &db) == SQLITE_OK);
    assert(sqlite3_exec(db,
        "CREATE TABLE sensor_rows (id INTEGER PRIMARY KEY AUTOINCREMENT, stream_id INTEGER NOT NULL, "
        "address TEXT NOT NULL, timestamp INTEGER NOT NULL, temp INTEGER NOT NULL, pressure INTEGER NOT NULL, "
        "flowRate INTEGER NOT NULL, massFlow INTEGER NOT NULL, volumeFlow INTEGER NOT NULL, "
        "density INTEGER NOT NULL, currentOfMotor INTEGER NOT NULL, percentageOfValve INTEGER NOT NULL);"
        "INSERT INTO sensor_rows(stream_id,address,timestamp,temp,pressure,flowRate,massFlow,volumeFlow,density,currentOfMotor,percentageOfValve) "
        "VALUES(0,'green',150,1,2,3,4,5,6,7,8);"
        "INSERT INTO sensor_rows(stream_id,address,timestamp,temp,pressure,flowRate,massFlow,volumeFlow,density,currentOfMotor,percentageOfValve) "
        "VALUES(1,'pink',250,11,12,13,14,15,16,17,18);", nullptr, nullptr, nullptr) == SQLITE_OK);
    sqlite3_close(db);

    tabletpipe::ReadDb reader(databasePath, binaryPath);
    auto first = reader.readSince(0);
    assert(first.ok() && first.records.size() == 2);
    assert(first.records[0].temp == 1 && first.records[1].temp == 11);
    assert(first.records[0].id == 1);
    auto second = reader.readRange({tabletpipe::RangeKey::Timestamp, 1, 150, 250});
    assert(second.ok() && second.records.size() == 2 && second.records[0].id == 1);
    auto empty = reader.readSince(2);
    assert(empty.ok() && empty.records.empty());

    std::remove(databasePath);
    std::remove(binaryPath);
    return 0;
}
