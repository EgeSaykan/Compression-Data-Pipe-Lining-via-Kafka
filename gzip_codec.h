// gzip_codec.h
//
// Decompresses gzip payloads coming from the phone (GzipCodec.kt /
// java.util.zip.GZIPOutputStream). Requires zlib -- on Windows via vcpkg:
//   vcpkg install zlib
// and in CMakeLists.txt:
//   find_package(ZLIB REQUIRED)
//   target_link_libraries(<target> PRIVATE ZLIB::ZLIB)

#pragma once

#include <cstdint>
#include <vector>

namespace phonepipe {

// Returns the decompressed bytes, or an empty vector on failure (check
// stderr for the zlib error).
std::vector<uint8_t> gzipDecompress(const uint8_t* data, size_t len);

} // namespace phonepipe
