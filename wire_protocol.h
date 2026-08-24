// wire_protocol.h
//
// Shared definitions for the phone -> PC Bluetooth SPP wire format.
// Included by both receiver.cpp (Bluetooth -> Kafka producer) and
// kafka_db_writer.cpp (Kafka -> binary file + SQLite consumer), so the
// two programs can never drift out of sync on the packet layout.
//
// Packet layout (unchanged fields keep their original byte offsets;
// initialTimeMs/endTimeMs are new and always sent UNCOMPRESSED,
// regardless of the compressed flag in byte 0):
//
//   byte 0        : 0x01 = OpenZL compressed, 0x00 = raw columnar (delta-encoded)
//   byte 1        : streamId (0 = green, 1 = pink)
//   bytes 2..5    : rowCount             uint32 big-endian
//   bytes 6..9    : payloadLen           uint32 big-endian
//   bytes 10..17  : initialTimeMs        int64  big-endian (timestamp of first row in batch)
//   bytes 18..25  : endTimeMs            int64  big-endian (timestamp of last row in batch)
//   bytes 26..    : payload (payloadLen bytes)
//
// Raw columnar payload (when byte 0 == 0x00), each column delta-encoded
// on-device before sending:
//
//   column 0 : timestamp          int64_t[rowCount]
//   column 1 : temp               int64_t[rowCount]
//   column 2 : pressure           int64_t[rowCount]
//   column 3 : flowRate           int64_t[rowCount]
//   column 4 : massFlow           int64_t[rowCount]
//   column 5 : volumeFlow         int64_t[rowCount]
//   column 6 : density            int64_t[rowCount]
//   column 7 : currentOfMotor     int64_t[rowCount]
//   column 8 : percentageOfValve  int64_t[rowCount]

#pragma once

#include <cstdint>
#include <cstddef>
#include <string>

namespace phonepipe {

constexpr size_t kHeaderSize = 26;
constexpr int    kFieldCount = 9;
constexpr size_t kColWidth   = 8;

constexpr uint8_t STREAM_GREEN = 0;
constexpr uint8_t STREAM_PINK  = 1;

struct PacketHeader {
    bool     compressed = false;
    uint8_t  streamId = 0;
    uint32_t rowCount = 0;
    uint32_t payloadLen = 0;
    int64_t  initialTimeMs = 0;
    int64_t  endTimeMs = 0;
};

inline void putU32BE(uint8_t* p, uint32_t v) {
    p[0] = uint8_t(v >> 24);
    p[1] = uint8_t(v >> 16);
    p[2] = uint8_t(v >> 8);
    p[3] = uint8_t(v);
}

inline uint32_t getU32BE(const uint8_t* p) {
    return (uint32_t(p[0]) << 24) | (uint32_t(p[1]) << 16) |
           (uint32_t(p[2]) << 8)  |  uint32_t(p[3]);
}

inline void putI64BE(uint8_t* p, int64_t v) {
    const uint64_t u = static_cast<uint64_t>(v);
    for (int i = 0; i < 8; ++i) {
        p[i] = uint8_t(u >> (56 - 8 * i));
    }
}

inline int64_t getI64BE(const uint8_t* p) {
    uint64_t u = 0;
    for (int i = 0; i < 8; ++i) {
        u = (u << 8) | uint64_t(p[i]);
    }
    return static_cast<int64_t>(u);
}

inline PacketHeader decodeHeader(const uint8_t* h) {
    PacketHeader hdr;
    hdr.compressed    = h[0] == 0x01;
    hdr.streamId      = h[1];
    hdr.rowCount      = getU32BE(h + 2);
    hdr.payloadLen    = getU32BE(h + 6);
    hdr.initialTimeMs = getI64BE(h + 10);
    hdr.endTimeMs     = getI64BE(h + 18);
    return hdr;
}

inline void encodeHeader(uint8_t* h, const PacketHeader& hdr) {
    h[0] = hdr.compressed ? 0x01 : 0x00;
    h[1] = hdr.streamId;
    putU32BE(h + 2, hdr.rowCount);
    putU32BE(h + 6, hdr.payloadLen);
    putI64BE(h + 10, hdr.initialTimeMs);
    putI64BE(h + 18, hdr.endTimeMs);
}

inline const char* streamName(uint8_t streamId) {
    if (streamId == STREAM_GREEN) return "green";
    if (streamId == STREAM_PINK)  return "pink";
    return "unknown";
}

inline std::string kafkaTopicFor(uint8_t streamId) {
    return std::string("phonepipe.") + streamName(streamId);
}

} // namespace phonepipe
