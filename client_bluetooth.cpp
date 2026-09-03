#include "client_bluetooth.h"

#include "read_db.h"
#include "tablet_protocol.h"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <chrono>
#include <limits>

#include "openzl_codec.h"
#include "wire_protocol.h"

using  phonepipe::kFieldCount;

#pragma comment(lib, "Ws2_32.lib")

namespace tabletpipe {
namespace {

const GUID kTabletSppUuid = {
    0x7d3f9a21, 0x8c4b, 0x4d6e,
    {0xa1, 0x72, 0x35, 0x9c, 0x04, 0x6b, 0x8e, 0xf0}
};

constexpr size_t kBatchBodySize = 20;
constexpr size_t kLiveBodySize = 12;
constexpr size_t kMaxErrorMessage = 1024;
constexpr int kTabletFieldCount = kFieldCount + 1;

using SteadyClock = std::chrono::steady_clock;

int64_t elapsedMs(SteadyClock::time_point start) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        SteadyClock::now() - start).count();
}

// Flattens records into the tablet payload: pipe ID followed by the sensor
// columns, with rowCount int64 elements per column in column-major order.
std::vector<int64_t> flattenColumnar(const std::vector<Record>& records) {
    const size_t rowCount = records.size();
    std::vector<int64_t> flat(static_cast<size_t>(kTabletFieldCount) * rowCount);
    int64_t* pipeId = flat.data();
    int64_t* timestamp = pipeId + rowCount;
    int64_t* temp = timestamp + rowCount;
    int64_t* pressure = temp + rowCount;
    int64_t* flowRate = pressure + rowCount;
    int64_t* massFlow = flowRate + rowCount;
    int64_t* volumeFlow = massFlow + rowCount;
    int64_t* density = volumeFlow + rowCount;
    int64_t* currentOfMotor = density + rowCount;
    int64_t* percentageOfValve = currentOfMotor + rowCount;

    for (size_t i = 0; i < rowCount; ++i) {
        const Record& record = records[i];
        pipeId[i] = record.streamId;
        timestamp[i] = record.timestamp;
        temp[i] = record.temp;
        pressure[i] = record.pressure;
        flowRate[i] = record.flowRate;
        massFlow[i] = record.massFlow;
        volumeFlow[i] = record.volumeFlow;
        density[i] = record.density;
        currentOfMotor[i] = record.currentOfMotor;
        percentageOfValve[i] = record.percentageOfValve;
    }
    return flat;
}

constexpr size_t kLiveBatchFlushLimit = 12;

bool readExact(SOCKET socket, uint8_t* buffer, size_t size) {
    size_t offset = 0;
    while (offset < size) {
        const int count = recv(socket, reinterpret_cast<char*>(buffer + offset),
                               static_cast<int>(size - offset), 0);
        if (count <= 0) return false;
        offset += static_cast<size_t>(count);
    }
    return true;
}

std::vector<uint8_t> bodyFor(const std::vector<uint8_t>& payload) {
    if (payload.size() < 1) return {};
    return std::vector<uint8_t>(payload.begin() + 1, payload.end());
}

} // namespace

TabletServer::TabletServer(std::string databasePath, std::string binaryPath)
    : databasePath_(std::move(databasePath)), binaryPath_(std::move(binaryPath)) {
    WSADATA data{};
    const int result = WSAStartup(MAKEWORD(2, 2), &data);
    wsaStarted_ = result == 0;
    if (!wsaStarted_) {
        std::fprintf(stderr, "Tablet WSAStartup failed: %d\n", result);
        return;
    }
    server_ = createBluetoothServer();
}

TabletServer::~TabletServer() {
    stop();
    if (server_ != INVALID_SOCKET) closesocket(server_);
    if (wsaStarted_) WSACleanup();
}

bool TabletServer::ok() const {
    return wsaStarted_ && server_ != INVALID_SOCKET;
}

bool TabletServer::isLiveClientConnected() const {
    return liveClientConnected_.load(std::memory_order_relaxed);
}

void TabletServer::offerLivePacket(uint8_t streamId, const std::vector<uint8_t>& packet,
                                    uint32_t rowCount) {
    if (!liveClientConnected_.load(std::memory_order_relaxed) ||
        !liveModeActive_.load(std::memory_order_relaxed)) return;
    std::lock_guard<std::mutex> lock(queueMutex_);
    if (!liveClientConnected_.load(std::memory_order_relaxed) ||
        !liveModeActive_.load(std::memory_order_relaxed)) return;
    if (liveQueue_.size() >= kLiveQueueCapacity) liveQueue_.pop_front();
    liveQueue_.push_back({streamId, rowCount, packet});
}

void TabletServer::stop() {
    stopping_.store(true, std::memory_order_relaxed);
    clearLiveState();
    SOCKET client = client_.exchange(INVALID_SOCKET);
    if (client != INVALID_SOCKET) shutdown(client, SD_BOTH);
    if (server_ != INVALID_SOCKET) shutdown(server_, SD_BOTH);
}

bool TabletServer::registerSppService(int port) {
    SOCKADDR_BTH local{};
    local.addressFamily = AF_BTH;
    local.serviceClassId = kTabletSppUuid;
    local.port = port;

    CSADDR_INFO address{};
    address.iProtocol = BTHPROTO_RFCOMM;
    address.iSocketType = SOCK_STREAM;
    address.LocalAddr.lpSockaddr = reinterpret_cast<LPSOCKADDR>(&local);
    address.LocalAddr.iSockaddrLength = sizeof(local);

    WSAQUERYSETW query{};
    query.dwSize = sizeof(query);
    query.dwNameSpace = NS_BTH;
    query.lpServiceClassId = const_cast<LPGUID>(&kTabletSppUuid);
    query.lpszServiceInstanceName = const_cast<LPWSTR>(L"Ege Sensor Tablet SPP");
    query.dwNumberOfCsAddrs = 1;
    query.lpcsaBuffer = &address;
    if (WSASetServiceW(&query, RNRSERVICE_REGISTER, 0) != 0) {
        std::fprintf(stderr, "Tablet WSASetService failed: %d\n", WSAGetLastError());
        return false;
    }
    return true;
}

SOCKET TabletServer::createBluetoothServer() {
    SOCKET socketHandle = socket(AF_BTH, SOCK_STREAM, BTHPROTO_RFCOMM);
    if (socketHandle == INVALID_SOCKET) return INVALID_SOCKET;

    SOCKADDR_BTH address{};
    address.addressFamily = AF_BTH;
    address.serviceClassId = kTabletSppUuid;
    address.port = BT_PORT_ANY;
    if (bind(socketHandle, reinterpret_cast<SOCKADDR*>(&address), sizeof(address)) == SOCKET_ERROR) {
        closesocket(socketHandle);
        return INVALID_SOCKET;
    }
    SOCKADDR_BTH bound{};
    int length = sizeof(bound);
    if (getsockname(socketHandle, reinterpret_cast<SOCKADDR*>(&bound), &length) == SOCKET_ERROR ||
        listen(socketHandle, 1) == SOCKET_ERROR || !registerSppService(bound.port)) {
        closesocket(socketHandle);
        return INVALID_SOCKET;
    }
    std::printf("Tablet Bluetooth SPP listening on RFCOMM channel %d\n", bound.port);
    return socketHandle;
}

void TabletServer::run() {
    if (!ok()) return;
    while (!stopping_.load(std::memory_order_relaxed)) {
        SOCKADDR_BTH address{};
        int length = sizeof(address);
        SOCKET client = accept(server_, reinterpret_cast<SOCKADDR*>(&address), &length);
        if (client == INVALID_SOCKET) {
            if (!stopping_.load(std::memory_order_relaxed))
                std::fprintf(stderr, "Tablet accept failed: %d\n", WSAGetLastError());
            continue;
        }
        SOCKET expected = INVALID_SOCKET;
        if (!client_.compare_exchange_strong(expected, client)) {
            sendError(client, "another tablet client is already connected");
            closesocket(client);
            continue;
        }
        liveClientConnected_.store(true, std::memory_order_relaxed);
        std::printf("Tablet client connected\n");
        serveClient(client);
        clearLiveState();
        client_.compare_exchange_strong(client, INVALID_SOCKET);
        closesocket(client);
        std::printf("Tablet client disconnected\n");
    }
}

void TabletServer::serveClient(SOCKET client) {
    while (!stopping_.load(std::memory_order_relaxed)) {
        std::vector<uint8_t> payload;
        if (!receiveFrame(client, payload, -1)) break;
        if (!handleRequest(client, payload)) break;
    }
}

bool TabletServer::sendAggregatedRecords(SOCKET client, const std::vector<Record>& records) {
    if (records.empty()) return true;
    if (records.size() > std::numeric_limits<uint32_t>::max()) return false;

    const uint32_t totalRows = static_cast<uint32_t>(records.size());
    int64_t firstTimestamp = records.front().timestamp;
    for (const Record& record : records) {
        firstTimestamp = std::min(firstTimestamp, record.timestamp);
    }

    const std::vector<int64_t> flat = flattenColumnar(records);
    const std::vector<uint8_t> compressed = phonepipe::openzlCompressFresh(
        reinterpret_cast<const uint8_t*>(flat.data()), flat.size() * sizeof(int64_t),
        static_cast<int>(totalRows), kTabletFieldCount);
    if (compressed.empty()) return false;

    std::vector<uint8_t> batchBody(28 + compressed.size());
    batchBody[0] = kProtocolVersion;
    batchBody[1] = records.front().streamId;
    putU16LE(batchBody.data() + 2, 0);
    putI64LE(batchBody.data() + 4, records.front().id);
    putI64LE(batchBody.data() + 12, firstTimestamp);
    putU32LE(batchBody.data() + 20, totalRows);
    putU32LE(batchBody.data() + 24, static_cast<uint32_t>(compressed.size()));
    std::copy(compressed.begin(), compressed.end(), batchBody.begin() + 28);

    const auto frame = makeFrame(FrameType::DataRecord, batchBody);
    std::printf("Historical package: records=%zu rows=%u compressed=%zu bytes body=%zu bytes frame=%zu bytes\n",
                records.size(), totalRows, compressed.size(), batchBody.size(), frame.size());
    return !frame.empty() && sendAll(client, frame.data(), frame.size());
}

bool TabletServer::handleRequest(SOCKET client, const std::vector<uint8_t>& payload) {
    if (payload.empty()) return sendError(client, "empty request");
    const auto type = static_cast<FrameType>(payload[0]);
    const std::vector<uint8_t> body = bodyFor(payload);
    std::printf("Received request: %s\n", frameTypeName(type).c_str());
    switch (type) {
        case FrameType::BatchRequest: return handleBatch(client, body);
        case FrameType::LiveRequest: return handleLive(client, body);
        case FrameType::Stop:
            return body.size() == 1 && body[0] == kProtocolVersion ? false
                                                                    : sendError(client, "malformed stop request");
        default: return sendError(client, "unsupported request frame");
    }
}

bool TabletServer::handleBatch(SOCKET client, const std::vector<uint8_t>& body) {
    const auto batchStart = SteadyClock::now();
    printf("hh1\n");
    if (body.size() != kBatchBodySize || body[0] != kProtocolVersion ||
        (body[1] != static_cast<uint8_t>(RequestRangeKey::Id) &&
         body[1] != static_cast<uint8_t>(RequestRangeKey::Timestamp))) {
        return sendError(client, "malformed batch request");
    }
    printf("hh2\n");
    RecordRange range;
    range.key = body[1] == 0 ? RangeKey::Id : RangeKey::Timestamp;
    range.streamId = body[2];
    range.start = getI64LE(body.data() + 4);
    range.end = getI64LE(body.data() + 12);
    printf("Batch request: key=%s streamId=%u start=%lld end=%lld\n",
           range.key == RangeKey::Id ? "id" : "timestamp",
           static_cast<unsigned>(range.streamId),
           static_cast<long long>(range.start),
           static_cast<long long>(range.end));
    ReadDb reader(databasePath_, binaryPath_);
    const auto readStart = SteadyClock::now();
    ReadResult result = reader.readRange(range);
    std::printf("[BatchTiming] readRange records=%zu elapsed=%lldms\n",
                result.records.size(), static_cast<long long>(elapsedMs(readStart)));
    if (!result.ok()) return sendError(client, result.error);
    printf("hh3\n");

    std::printf("Sending %zu historical records\n", result.records.size());
    size_t totalBytes = 0;
    int64_t recoveryMs = 0;
    int64_t sendMs = 0;
    if (!sendAggregatedRecords(client, result.records)) {
        printf("Failed to send historical batch payload\n");
        return sendError(client, "historical batch payload is invalid");
    }
    printf("not failed to send historical batch payload\n");
    const auto end = makeFrame(FrameType::BatchEnd, {kProtocolVersion});
    const auto endSendStart = SteadyClock::now();
    const bool sent = !end.empty() && sendAll(client, end.data(), end.size());
    sendMs += elapsedMs(endSendStart);
    totalBytes += end.size();
    std::printf("[BatchTiming] recovery=%lldms send=%lldms total=%lldms bytes=%zu\n",
                static_cast<long long>(recoveryMs), static_cast<long long>(sendMs),
                static_cast<long long>(elapsedMs(batchStart)), totalBytes);
    return sent;
}

bool TabletServer::handleLive(SOCKET client, const std::vector<uint8_t>& requestBody) {
    if (requestBody.size() != kLiveBodySize || requestBody[0] != kProtocolVersion ||
        (requestBody[1] != 0 && requestBody[1] != 1)) return sendError(client, "malformed live request");

    ReadDb reader(databasePath_, binaryPath_);
    if (!requestBody[1]) {
        ReadResult backfill = reader.readSince(getI64LE(requestBody.data() + 4));
        if (!backfill.ok()) return sendError(client, backfill.error);
        std::printf("Sending %zu backfill records\n", backfill.records.size());
        if (!sendAggregatedRecords(client, backfill.records)) {
            return sendError(client, "backfill batch payload is invalid");
        }
    }

    std::printf("Live mode active\n");
    liveModeActive_.store(true, std::memory_order_relaxed);
    while (!stopping_.load(std::memory_order_relaxed)) {
        fd_set readable;
        FD_ZERO(&readable);
        FD_SET(client, &readable);
        timeval timeout{0, 50000};
        const int ready = select(0, &readable, nullptr, nullptr, &timeout);
        if (ready == SOCKET_ERROR) return false;

        if (ready > 0 && FD_ISSET(client, &readable)) {
            std::vector<uint8_t> request;
            if (!receiveFrame(client, request, -1)) return false;
            if (request.size() >= 1 && request[0] == static_cast<uint8_t>(FrameType::Stop)) {
                std::printf("Received Stop request\n");
                return false;
            }
            return sendError(client, "only stop is valid during live mode");
        }

        std::vector<QueuedPacket> batch;
        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            while (!liveQueue_.empty() && batch.size() < kLiveBatchFlushLimit) {
                batch.push_back(std::move(liveQueue_.front()));
                liveQueue_.pop_front();
            }
        }
        if (batch.empty()) continue;

        for (QueuedPacket& queued : batch) {
            // Header size 12: v(1), sid(1), res(2), rc(4), plen(4)
            std::vector<uint8_t> packetBody(12 + queued.packet.size());
            packetBody[0] = kProtocolVersion;
            packetBody[1] = queued.streamId;
            putU16LE(packetBody.data() + 2, 0); // reserved
            putU32LE(packetBody.data() + 4, queued.rowCount);
            putU32LE(packetBody.data() + 8, static_cast<uint32_t>(queued.packet.size()));
            std::copy(queued.packet.begin(), queued.packet.end(), packetBody.begin() + 12);

            const auto frame = makeFrame(FrameType::LivePacket, packetBody);
            if (frame.empty() || !sendAll(client, frame.data(), frame.size())) {
                std::printf("Failed to send live packet\n");
                return false;
            }
        }
    }
    return false;
}

bool TabletServer::sendAll(SOCKET socketHandle, const uint8_t* data, size_t size) {
    size_t offset = 0;
    while (offset < size) {
        const int count = send(socketHandle, reinterpret_cast<const char*>(data + offset),
                               static_cast<int>(size - offset), 0);
        if (count <= 0) return false;
        offset += static_cast<size_t>(count);
    }
    return true;
}

bool TabletServer::receiveFrame(SOCKET socketHandle, std::vector<uint8_t>& payload, int timeoutMs) {
    if (timeoutMs >= 0) {
        timeval timeout{timeoutMs / 1000, (timeoutMs % 1000) * 1000};
        fd_set readable;
        FD_ZERO(&readable);
        FD_SET(socketHandle, &readable);
        const int ready = select(0, &readable, nullptr, nullptr, &timeout);
        if (ready <= 0) return false;
    }
    uint8_t lengthBytes[4];
    if (!readExact(socketHandle, lengthBytes, sizeof(lengthBytes))) return false;
    const uint32_t length = getU32LE(lengthBytes);
    if (!parseLength(length)) return false;
    payload.resize(length);
    return readExact(socketHandle, payload.data(), payload.size());
}

bool TabletServer::sendError(SOCKET client, const std::string& message) {
    const size_t length = std::min(message.size(), kMaxErrorMessage);
    std::vector<uint8_t> body(3 + length);
    body[0] = kProtocolVersion;
    putU16LE(body.data() + 1, static_cast<uint16_t>(length));
    std::copy(message.begin(), message.begin() + length, body.begin() + 3);
    const auto frame = makeFrame(FrameType::Error, body);
    return !frame.empty() && sendAll(client, frame.data(), frame.size());
}

void TabletServer::clearLiveState() {
    liveModeActive_.store(false, std::memory_order_relaxed);
    liveClientConnected_.store(false, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lock(queueMutex_);
    liveQueue_.clear();
}

} // namespace tabletpipe
