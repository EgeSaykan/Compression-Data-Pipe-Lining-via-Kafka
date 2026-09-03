#include "openzl_codec.h"
#include "wire_protocol.h"

#include <cstdio>
#include <cstring>
#include <mutex>

#include "openzl/zl_compress.h"
#include "openzl/zl_compressor.h"
#include "openzl/zl_public_nodes.h"
#include "openzl/zl_errors.h"

namespace phonepipe {
namespace {

// Aligned scratch buffer so ZL_TypedRef_createNumeric's alignment
// requirement is always satisfied.
struct AlignedColumns {
    std::vector<int64_t> data;
    explicit AlignedColumns(size_t totalInt64s) : data(totalInt64s) {}
    uint8_t* bytes() { return reinterpret_cast<uint8_t*>(data.data()); }
};

ZL_Compressor* g_compressor = nullptr;
std::once_flag g_compressorInitFlag;

bool initCompressor() {
    ZL_Compressor* c = ZL_Compressor_create();
    if (!c) {
        fprintf(stderr, "openzl_codec: ZL_Compressor_create failed\n");
        return false;
    }
    ZL_Report fmtReport =
            ZL_Compressor_setParameter(c, ZL_CParam_formatVersion, ZL_MAX_FORMAT_VERSION);
    if (ZL_isError(fmtReport)) {
        fprintf(stderr, "openzl_codec: setParameter(formatVersion) failed: %s\n",
                ZL_Compressor_getErrorContextString(c, fmtReport));
        ZL_Compressor_free(c);
        return false;
    }
    ZL_Report graphReport =
            ZL_Compressor_selectStartingGraphID(c, ZL_GRAPH_COMPRESS_GENERIC);
    if (ZL_isError(graphReport)) {
        fprintf(stderr, "openzl_codec: selectStartingGraphID failed: %s\n",
                ZL_Compressor_getErrorContextString(c, graphReport));
        ZL_Compressor_free(c);
        return false;
    }
    g_compressor = c;
    return true;
}

ZL_Compressor* getCompressor() {
    std::call_once(g_compressorInitFlag, [] { initCompressor(); });
    return g_compressor;
}

void deltaEncodeInPlace(int64_t* col, int rowCount) {
    for (int i = rowCount - 1; i > 0; i--) {
        col[i] = col[i] - col[i - 1];
    }
}

// Shared entropy-compression stage. `aligned` must already hold
// (possibly delta-encoded) columnar data.
std::vector<uint8_t> entropyCompress(AlignedColumns& aligned, size_t expectedLen,
                                     int rowCount, int fieldCount) {
    std::vector<uint8_t> out;

    ZL_Compressor* compressor = getCompressor();
    if (!compressor) {
        fprintf(stderr, "openzl_codec: no compressor available\n");
        return out;
    }

    const size_t colBytes = static_cast<size_t>(rowCount) * kColWidth;
    std::vector<const ZL_TypedRef*> refs(fieldCount, nullptr);
    for (int c = 0; c < fieldCount; c++) {
        uint8_t* colStart = aligned.bytes() + c * colBytes;
        refs[c] = ZL_TypedRef_createNumeric(colStart, kColWidth, static_cast<size_t>(rowCount));
        if (!refs[c]) {
            fprintf(stderr, "openzl_codec: ZL_TypedRef_createNumeric failed for column %d\n", c);
            for (auto* r : refs) if (r) ZL_TypedRef_free(const_cast<ZL_TypedRef*>(r));
            return out;
        }
    }

    ZL_CCtx* cctx = ZL_CCtx_create();
    if (!cctx) {
        fprintf(stderr, "openzl_codec: ZL_CCtx_create failed\n");
        for (auto* r : refs) if (r) ZL_TypedRef_free(const_cast<ZL_TypedRef*>(r));
        return out;
    }

    ZL_Report refReport = ZL_CCtx_refCompressor(cctx, compressor);
    if (ZL_isError(refReport)) {
        fprintf(stderr, "openzl_codec: refCompressor failed: %s\n",
                ZL_CCtx_getErrorContextString(cctx, refReport));
        for (auto* r : refs) if (r) ZL_TypedRef_free(const_cast<ZL_TypedRef*>(r));
        ZL_CCtx_free(cctx);
        return out;
    }

    const size_t dstCapacity = expectedLen + expectedLen / 2 + 4096;
    std::vector<uint8_t> dst(dstCapacity);

    ZL_Report report = ZL_CCtx_compressMultiTypedRef(
            cctx, dst.data(), dst.size(), refs.data(), refs.size());

    if (ZL_isError(report)) {
        fprintf(stderr, "openzl_codec: compress failed: %s\n",
                ZL_CCtx_getErrorContextString(cctx, report));
    } else {
        const size_t compressedSize = ZL_validResult(report);
        out.assign(dst.begin(), dst.begin() + compressedSize);
    }

    for (auto* r : refs) if (r) ZL_TypedRef_free(const_cast<ZL_TypedRef*>(r));
    ZL_CCtx_free(cctx);
    return out;
}

} // namespace

std::vector<uint8_t> openzlCompressDeltaEncoded(const uint8_t* data, size_t len, int rowCount, int numCols) {
    std::vector<uint8_t> out;
    if (rowCount <= 0) return out;

    if (numCols < 0) {
        if ((len % (static_cast<size_t>(rowCount) * kColWidth)) != 0) {
            fprintf(stderr, "openzlCompressDeltaEncoded: bad input size %zu for rowCount %d\n", len, rowCount);
            return out;
        }
        numCols = static_cast<int>(len / (static_cast<size_t>(rowCount) * kColWidth));
    }

    const size_t expectedLen = static_cast<size_t>(rowCount) * kColWidth * numCols;
    if (len != expectedLen) {
        fprintf(stderr, "openzlCompressDeltaEncoded: bad input size %zu, expected %zu\n", len, expectedLen);
        return out;
    }

    AlignedColumns aligned(len / sizeof(int64_t));
    std::memcpy(aligned.bytes(), data, len);
    // Already delta-encoded on-device -- straight to entropy coding.
    return entropyCompress(aligned, len, rowCount, numCols);
}

std::vector<uint8_t> openzlCompressFresh(const uint8_t* data, size_t len, int rowCount, int numCols) {
    std::vector<uint8_t> out;
    if (rowCount <= 0) return out;

    if (numCols < 0) {
        if ((len % (static_cast<size_t>(rowCount) * kColWidth)) != 0) {
            fprintf(stderr, "openzlCompressFresh: bad input size %zu for rowCount %d\n", len, rowCount);
            return out;
        }
        numCols = static_cast<int>(len / (static_cast<size_t>(rowCount) * kColWidth));
    }

    const size_t expectedLen = static_cast<size_t>(rowCount) * kColWidth * numCols;
    if (len != expectedLen) {
        fprintf(stderr, "openzlCompressFresh: bad input size %zu, expected %zu\n", len, expectedLen);
        return out;
    }

    AlignedColumns aligned(len / sizeof(int64_t));
    std::memcpy(aligned.bytes(), data, len);

    const size_t colBytes = static_cast<size_t>(rowCount) * kColWidth;
    for (int c = 0; c < numCols; c++) {
        auto* colPtr = reinterpret_cast<int64_t*>(aligned.bytes() + c * colBytes);
        deltaEncodeInPlace(colPtr, rowCount);
    }

    return entropyCompress(aligned, len, rowCount, numCols);
}

} // namespace phonepipe
