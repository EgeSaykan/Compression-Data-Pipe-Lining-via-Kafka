#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

void writeBytes(const fs::path& path, const std::vector<uint8_t>& bytes) {
    std::ofstream output(path, std::ios::binary);
    if (!output) throw std::runtime_error("Could not create " + path.string());
    output.write(reinterpret_cast<const char*>(bytes.data()),
                 static_cast<std::streamsize>(bytes.size()));
}

void writeRandomFile(const fs::path& path, size_t size, std::mt19937_64& random) {
    std::vector<uint8_t> bytes(size);
    for (auto& byte : bytes) byte = static_cast<uint8_t>(random() & 0xff);
    writeBytes(path, bytes);
}

void writeTrendFile(const fs::path& path, size_t size, std::mt19937_64& random) {
    const size_t valueCount = size / sizeof(int64_t);
    std::vector<int64_t> values(valueCount);
    std::uniform_int_distribution<int64_t> startDistribution(-1000000000LL, 1000000000LL);
    std::uniform_int_distribution<int> stepDistribution(1, 7);
    std::bernoulli_distribution increaseDistribution(0.5);

    int64_t value = startDistribution(random);
    int direction = increaseDistribution(random) ? 1 : -1;
    for (size_t index = 0; index < valueCount; ++index) {
        values[index] = value;
        value += direction * stepDistribution(random);
        if ((index + 1) % 257 == 0 && increaseDistribution(random)) direction = -direction;
    }

    std::vector<uint8_t> bytes(size);
    std::memcpy(bytes.data(), values.data(), bytes.size());
    writeBytes(path, bytes);
}

} // namespace

int main(int argc, char** argv) {
    const fs::path outputDirectory = argc > 1 ? argv[1] : "inputs";
    const uint64_t seed = argc > 2 ? std::stoull(argv[2]) : std::random_device{}();

    fs::create_directories(outputDirectory);
    std::mt19937_64 random(seed);
    const std::vector<size_t> sizes = {
        1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216
    };

    for (size_t size : sizes) {
        writeRandomFile(outputDirectory / ("random_" + std::to_string(size) + ".bin"), size, random);
        writeTrendFile(outputDirectory / ("trend_" + std::to_string(size) + ".bin"), size, random);
    }

    std::cout << "Generated " << sizes.size() * 2 << " files in "
              << outputDirectory << " using seed " << seed << "\n";
}