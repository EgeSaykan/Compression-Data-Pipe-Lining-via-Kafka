#include "gzip_codec.h"

#include <cstdio>
#include <zlib.h>

namespace phonepipe {

std::vector<uint8_t> gzipDecompress(const uint8_t* data, size_t len) {
    std::vector<uint8_t> out;
    if (len == 0) return out;

    z_stream strm{};
    // windowBits = 15 + 16 tells zlib to expect/parse a gzip header
    // (as opposed to raw deflate or zlib-wrapped deflate).
    if (inflateInit2(&strm, 15 + 16) != Z_OK) {
        fprintf(stderr, "gzipDecompress: inflateInit2 failed\n");
        return out;
    }

    strm.next_in = const_cast<Bytef*>(data);
    strm.avail_in = static_cast<uInt>(len);

    std::vector<uint8_t> chunk(64 * 1024);
    int ret;
    do {
        strm.next_out = chunk.data();
        strm.avail_out = static_cast<uInt>(chunk.size());

        ret = inflate(&strm, Z_NO_FLUSH);
        if (ret != Z_OK && ret != Z_STREAM_END) {
            fprintf(stderr, "gzipDecompress: inflate failed (%d): %s\n",
                    ret, strm.msg ? strm.msg : "unknown error");
            inflateEnd(&strm);
            out.clear();
            return out;
        }

        const size_t produced = chunk.size() - strm.avail_out;
        out.insert(out.end(), chunk.begin(), chunk.begin() + produced);
    } while (ret != Z_STREAM_END);

    inflateEnd(&strm);
    return out;
}

} // namespace phonepipe
