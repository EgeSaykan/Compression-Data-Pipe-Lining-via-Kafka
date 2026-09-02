// wire_protocol.h
//
// Shared definitions for the phone -> PC Bluetooth SPP wire format.
// Included by both receiver.cpp (Bluetooth -> Kafka producer) and
// kafka_db_writer.cpp (Kafka -> binary file + SQLite consumer), so the
// two programs can never drift out of sync on the packet layout.
//
// Packet layout (unchanged fields keep their original byte offsets):
//
//   byte 0        : CompressionFlag -- 0x00 = raw columnar (delta-encoded),
//                                       0x01 = OpenZL compressed,
//                                       0x02 = gzip compressed (NOT delta-encoded)
//   byte 1        : virtual streamId (even IDs = green, odd IDs = pink)
//   bytes 2..5    : rowCount             uint32 big-endian
//   bytes 6..9    : payloadLen           uint32 big-endian
//   bytes 10..    : payload (payloadLen bytes)
//
// Raw columnar payload (when byte 0 == RAW), each column delta-encoded
// on-device before sending. Gzip payload (byte 0 == GZIP) is the gzip of
// the plain (non-delta-encoded) columnar buffer -- see BluetoothReceiver::run,
// which normalizes both RAW and GZIP packets to OPENZL before they're
// handed to the tablet server / Kafka / RabbitMQ.
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
#include <vector>

namespace phonepipe {

constexpr size_t kHeaderSize = 10;
constexpr size_t kLegacyReceivedTimeSize = 8;
constexpr size_t kReceivedTimeSize = 16;
constexpr int    kFieldCount = 9;
constexpr size_t kColWidth   = 8;

constexpr uint8_t STREAM_GREEN = 0;
constexpr uint8_t STREAM_PINK  = 1;

// Matches CompressionMode.kt / BluetoothSender.kt's FLAG_* constants on the phone.
enum class CompressionFlag : uint8_t {
    RAW    = 0x00,
    OPENZL = 0x01,
    GZIP   = 0x02,
};

inline bool isValidStream(uint8_t streamId) {
    return streamId <= 255;
}

inline uint8_t parentStream(uint8_t streamId) {
    return streamId % 2 == 0 ? STREAM_GREEN : STREAM_PINK;
}

inline uint8_t virtualStreamId(uint8_t parentId, uint32_t copyIndex) {
    return static_cast<uint8_t>(copyIndex * 2 + (parentId == STREAM_PINK ? 1 : 0));
}

struct PacketHeader {
    CompressionFlag flag = CompressionFlag::RAW;
    uint8_t  streamId = 0;
    uint32_t rowCount = 0;
    uint32_t payloadLen = 0;
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

inline void appendReceivedTimes(std::vector<uint8_t>& packet,
                                int64_t receivedBeginTimeMs,
                                int64_t receivedEndTimeMs) {
    const size_t offset = packet.size();
    packet.resize(offset + kReceivedTimeSize);
    putI64BE(packet.data() + offset, receivedBeginTimeMs);
    putI64BE(packet.data() + offset + 8, receivedEndTimeMs);
}

inline int64_t getReceivedBeginTimeMs(const uint8_t* packet, size_t offset) {
    return getI64BE(packet + offset);
}

inline int64_t getReceivedEndTimeMs(const uint8_t* packet, size_t offset) {
    return getI64BE(packet + offset + 8);
}

inline PacketHeader decodeHeader(const uint8_t* h) {
    PacketHeader hdr;
    hdr.flag       = static_cast<CompressionFlag>(h[0]);
    hdr.streamId   = h[1];
    hdr.rowCount   = getU32BE(h + 2);
    hdr.payloadLen = getU32BE(h + 6);
    return hdr;
}

inline void encodeHeader(uint8_t* h, const PacketHeader& hdr) {
    h[0] = static_cast<uint8_t>(hdr.flag);
    h[1] = hdr.streamId;
    putU32BE(h + 2, hdr.rowCount);
    putU32BE(h + 6, hdr.payloadLen);
}

inline const char* streamName(uint8_t streamId) {
    if (parentStream(streamId) == STREAM_GREEN) return "green";
    if (parentStream(streamId) == STREAM_PINK)  return "pink";
    return "unknown";
}

inline const char* compressionName(CompressionFlag flag) {
    switch (flag) {
        case CompressionFlag::RAW:    return "raw";
        case CompressionFlag::OPENZL: return "openzl";
        case CompressionFlag::GZIP:   return "gzip";
    }
    return "unknown";
}

inline std::string kafkaTopicFor(uint8_t streamId) {
    return std::string("phonepipe.") +
           (parentStream(streamId) == STREAM_PINK ? "high" : "low");
}

} // namespace phonepipe
