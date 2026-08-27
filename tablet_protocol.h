#pragma once

#include <cstddef>
#include <cstdint>
#include <algorithm>
#include <string>
#include <vector>

namespace tabletpipe {

constexpr uint8_t kProtocolVersion = 1;
constexpr uint32_t kMaxFrameSize = 16U * 1024U * 1024U;
constexpr uint8_t kAnyStream = 0xff;

// Every frame is [uint32 payloadLength LE][uint8 type][type-specific body].
enum class FrameType : uint8_t {
    BatchRequest = 1,
    LiveRequest = 2,
    Stop = 3,
    DataRecord = 10,
    LivePacket = 11,
    BatchEnd = 12,
    Error = 13,
};

enum class RequestRangeKey : uint8_t {
    Id = 0,
    Timestamp = 1,
};

struct BatchRequest {
    RequestRangeKey key = RequestRangeKey::Id;
    uint8_t streamId = kAnyStream;
    int64_t start = 0;
    int64_t end = 0;
};

struct LiveRequest {
    bool now = true;
    int64_t sinceId = 0;
};

inline void putU16LE(uint8_t* p, uint16_t value) {
    p[0] = static_cast<uint8_t>(value);
    p[1] = static_cast<uint8_t>(value >> 8);
}

inline uint16_t getU16LE(const uint8_t* p) {
    return static_cast<uint16_t>(p[0]) |
           static_cast<uint16_t>(p[1]) << 8;
}

inline void putU32LE(uint8_t* p, uint32_t value) {
    for (int i = 0; i < 4; ++i) p[i] = static_cast<uint8_t>(value >> (8 * i));
}

inline uint32_t getU32LE(const uint8_t* p) {
    uint32_t value = 0;
    for (int i = 0; i < 4; ++i) value |= static_cast<uint32_t>(p[i]) << (8 * i);
    return value;
}

inline void putU64LE(uint8_t* p, uint64_t value) {
    for (int i = 0; i < 8; ++i) p[i] = static_cast<uint8_t>(value >> (8 * i));
}

inline uint64_t getU64LE(const uint8_t* p) {
    uint64_t value = 0;
    for (int i = 0; i < 8; ++i) value |= static_cast<uint64_t>(p[i]) << (8 * i);
    return value;
}

inline void putI64LE(uint8_t* p, int64_t value) {
    putU64LE(p, static_cast<uint64_t>(value));
}

inline int64_t getI64LE(const uint8_t* p) {
    return static_cast<int64_t>(getU64LE(p));
}

inline std::vector<uint8_t> makeFrame(FrameType type, const std::vector<uint8_t>& body) {
    if (body.size() > kMaxFrameSize - 1) return {};
    std::vector<uint8_t> frame(5 + body.size());
    putU32LE(frame.data(), static_cast<uint32_t>(body.size() + 1));
    frame[4] = static_cast<uint8_t>(type);
    std::copy(body.begin(), body.end(), frame.begin() + 5);
    return frame;
}

inline bool parseLength(uint32_t length) {
    return length >= 1 && length <= kMaxFrameSize;
}

inline std::string frameTypeName(FrameType type) {
    switch (type) {
        case FrameType::BatchRequest: return "batch request";
        case FrameType::LiveRequest: return "live request";
        case FrameType::Stop: return "stop";
        case FrameType::DataRecord: return "data record";
        case FrameType::LivePacket: return "live packet";
        case FrameType::BatchEnd: return "batch end";
        case FrameType::Error: return "error";
    }
    return "unknown";
}

} // namespace tabletpipe
