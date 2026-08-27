#pragma once

#include <cstdint>
#include <string>
#include <vector>

struct sqlite3;

namespace tabletpipe {

enum class RangeKey : uint8_t {
    Id = 0,
    Timestamp = 1,
};

struct RecordRange {
    RangeKey key = RangeKey::Id;
    uint8_t streamId = 0xff;
    int64_t start = 0;
    int64_t end = 0;
};

struct Record {
    int64_t id = 0;
    uint8_t streamId = 0;
    std::string address;
    int64_t beginIndex = 0;
    int64_t endIndex = 0;
    int64_t initialTime = 0;
    int64_t endTime = 0;
    int64_t receivedBeginTime = 0;
    int64_t receivedEndTime = 0;
    std::vector<uint8_t> compressedBytes;
};

struct ReadResult {
    std::vector<Record> records;
    std::string error;

    bool ok() const { return error.empty(); }
};

class ReadDb {
public:
    explicit ReadDb(std::string databasePath = "data/phonepipe.sqlite3",
                    std::string binaryPath = "data/phonepipe.bin");

    ReadResult readRange(const RecordRange& range) const;
    ReadResult readSince(int64_t cursorId, uint8_t streamId = 0xff) const;
    bool maxId(int64_t& id, std::string& error) const;

private:
    ReadResult readQuery(const std::string& sql, const std::vector<int64_t>& values) const;

    std::string databasePath_;
    std::string binaryPath_;
};

} // namespace tabletpipe
