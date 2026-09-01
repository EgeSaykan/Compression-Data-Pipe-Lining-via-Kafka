// openzl_codec.h
//
// PC-side OpenZL compression, used by BluetoothReceiver::run to normalize
// every incoming packet to OPENZL before it reaches the tablet server /
// Kafka / RabbitMQ. Mirrors the logic in the phone's openzl_jni.cpp, minus
// the JNI plumbing.
//
// Two entry points because RAW and GZIP payloads arrive in different states:
//   - RAW packets are already delta-encoded on-device (OpenZLBridge.deltaEncodeColumns),
//     so we only need the entropy-coding stage.
//   - GZIP packets are gzip of the *plain* columnar buffer (not delta-encoded),
//     so we need the full delta + entropy pipeline, same as the device's
//     OpenZLBridge.compressColumns.

#pragma once

#include <cstdint>
#include <vector>

namespace phonepipe {

// `data` must already be delta-encoded columnar (kFieldCount columns of
// rowCount int64/double-bitpattern elements each). Entropy-compresses only.
std::vector<uint8_t> openzlCompressDeltaEncoded(const uint8_t* data, size_t len, int rowCount);

// `data` is plain (non-delta-encoded) columnar data. Delta-encodes each
// column, then entropy-compresses -- matches the device's OPENZL mode exactly.
std::vector<uint8_t> openzlCompressFresh(const uint8_t* data, size_t len, int rowCount);

} // namespace phonepipe
