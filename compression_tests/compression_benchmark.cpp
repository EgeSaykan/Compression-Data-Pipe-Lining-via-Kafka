#include "../openzl_codec.h"

#include <zlib.h>
#include <zstd.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;

namespace {

struct Result {
    std::string technique;
    size_t compressedSize = 0;
    double milliseconds = 0;
    bool available = false;
};

std::vector<uint8_t> readFile(const fs::path& path) {
    std::ifstream input(path, std::ios::binary | std::ios::ate);
    if (!input) throw std::runtime_error("Could not open " + path.string());
    const auto size = input.tellg();
    input.seekg(0);
    std::vector<uint8_t> bytes(static_cast<size_t>(size));
    input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    return bytes;
}

template <typename Function>
Result timeCompression(const std::string& name, Function&& function) {
    const auto start = Clock::now();
    std::vector<uint8_t> compressed = function();
    const auto finish = Clock::now();
    Result result{name, compressed.size(),
                  std::chrono::duration<double, std::milli>(finish - start).count(),
                  !compressed.empty()};
    return result;
}

Result gzipCompress(const std::vector<uint8_t>& input) {
    return timeCompression("gzip", [&] {
        uLongf capacity = compressBound(static_cast<uLong>(input.size()));
        std::vector<uint8_t> output(capacity);
        if (compress2(output.data(), &capacity, input.data(), static_cast<uLong>(input.size()),
                      Z_DEFAULT_COMPRESSION) != Z_OK) return std::vector<uint8_t>{};
        output.resize(capacity);
        return output;
    });
}

Result zstdCompress(const std::vector<uint8_t>& input) {
    return timeCompression("zstandard", [&] {
        std::vector<uint8_t> output(ZSTD_compressBound(input.size()));
        const size_t size = ZSTD_compress(output.data(), output.size(), input.data(), input.size(), 3);
        if (ZSTD_isError(size)) return std::vector<uint8_t>{};
        output.resize(size);
        return output;
    });
}

Result openZlCompress(const std::vector<uint8_t>& input, bool trend) {
    return timeCompression("openzl", [&] {
        const int rows = static_cast<int>(input.size() / sizeof(int64_t));
        if (trend) {
            return phonepipe::openzlCompressFresh(input.data(), input.size(), rows, 1);
        }
        return phonepipe::openzlCompressDeltaEncoded(input.data(), input.size(), rows, 1);
    });
}

std::string shellQuote(const fs::path& path) {
    return "\"" + path.string() + "\"";
}

Result externalArchive(const std::string& name, const std::string& command,
                       const fs::path& input, const fs::path& archive) {
    const auto start = Clock::now();
    const int exitCode = std::system((command + " >NUL 2>&1").c_str());
    const auto finish = Clock::now();
    Result result{name, 0,
                  std::chrono::duration<double, std::milli>(finish - start).count(), false};
    if (exitCode == 0 && fs::exists(archive)) {
        result.compressedSize = fs::file_size(archive);
        result.available = result.compressedSize > 0;
    }
    std::error_code error;
    fs::remove(archive, error);
    return result;
}

Result sevenZipCompress(const fs::path& input, const fs::path& archive) {
    return externalArchive("7z", "7z a -bd -y -t7z " + shellQuote(archive) + " " + shellQuote(input),
                           input, archive);
}

Result zipCompress(const fs::path& input, const fs::path& archive) {
    return externalArchive("zip", "zip -q -j " + shellQuote(archive) + " " + shellQuote(input),
                           input, archive);
}

} // namespace

int main(int argc, char** argv) {
    const fs::path inputDirectory = argc > 1 ? argv[1] : "inputs";
    const fs::path resultDirectory = argc > 2 ? argv[2] : "results";
    fs::create_directories(resultDirectory);
    const fs::path temporaryDirectory = resultDirectory / "temporary";
    fs::create_directories(temporaryDirectory);

    std::cout << "file,original_bytes,technique,compressed_bytes,ratio,time_ms\n";
    for (const auto& entry : fs::directory_iterator(inputDirectory)) {
        if (entry.path().extension() != ".bin") continue;
        const std::vector<uint8_t> input = readFile(entry.path());
        const bool trend = entry.path().filename().string().rfind("trend_", 0) == 0;
        const std::string stem = entry.path().stem().string();
        std::vector<Result> results;
        results.push_back(openZlCompress(input, trend));
        results.push_back(gzipCompress(input));
        results.push_back(zstdCompress(input));
        results.push_back(sevenZipCompress(entry.path(), temporaryDirectory / (stem + ".7z")));
        results.push_back(zipCompress(entry.path(), temporaryDirectory / (stem + ".zip")));

        for (const Result& result : results) {
            std::cout << entry.path().filename().string() << ',' << input.size() << ','
                      << result.technique << ',';
            if (result.available) {
                std::cout << result.compressedSize << ','
                          << std::fixed << std::setprecision(4)
                          << static_cast<double>(result.compressedSize) / input.size() << ',';
            } else {
                std::cout << "NA,NA,";
            }
            std::cout << std::fixed << std::setprecision(3) << result.milliseconds << '\n';
        }
    }

    std::error_code error;
    fs::remove(temporaryDirectory, error);
}