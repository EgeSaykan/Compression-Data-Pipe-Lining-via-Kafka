#include "client_bluetooth.h"

#include "read_db.h"
#include "tablet_protocol.h"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <limits>

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
                                   int64_t receivedBeginTimeMs, int64_t receivedEndTimeMs) {
    if (!liveClientConnected_.load(std::memory_order_relaxed) ||
        !liveModeActive_.load(std::memory_order_relaxed)) return;
    std::lock_guard<std::mutex> lock(queueMutex_);
    if (!liveClientConnected_.load(std::memory_order_relaxed) ||
        !liveModeActive_.load(std::memory_order_relaxed)) return;
    if (liveQueue_.size() >= kLiveQueueCapacity) liveQueue_.pop_front();
    liveQueue_.push_back({streamId, receivedBeginTimeMs, receivedEndTimeMs, packet});
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
        serveClient(client);
        clearLiveState();
        client_.compare_exchange_strong(client, INVALID_SOCKET);
        closesocket(client);
    }
}

void TabletServer::serveClient(SOCKET client) {
    while (!stopping_.load(std::memory_order_relaxed)) {
        std::vector<uint8_t> payload;
        if (!receiveFrame(client, payload, -1)) break;
        if (!handleRequest(client, payload)) break;
    }
}

bool TabletServer::handleRequest(SOCKET client, const std::vector<uint8_t>& payload) {
    if (payload.empty()) return sendError(client, "empty request");
    const auto type = static_cast<FrameType>(payload[0]);
    const std::vector<uint8_t> body = bodyFor(payload);
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
    if (body.size() != kBatchBodySize || body[0] != kProtocolVersion ||
        (body[1] != static_cast<uint8_t>(RequestRangeKey::Id) &&
         body[1] != static_cast<uint8_t>(RequestRangeKey::Timestamp))) {
        return sendError(client, "malformed batch request");
    }
    RecordRange range;
    range.key = body[1] == 0 ? RangeKey::Id : RangeKey::Timestamp;
    range.streamId = body[2];
    range.start = getI64LE(body.data() + 4);
    range.end = getI64LE(body.data() + 12);
    ReadDb reader(databasePath_, binaryPath_);
    ReadResult result = reader.readRange(range);
    if (!result.ok()) return sendError(client, result.error);
    for (const Record& record : result.records) {
        std::vector<uint8_t> recordBody(48 + record.compressedBytes.size());
        recordBody[0] = kProtocolVersion;
        recordBody[1] = record.streamId;
        putU16LE(recordBody.data() + 2, 0);
        putI64LE(recordBody.data() + 4, record.id);
        putI64LE(recordBody.data() + 12, record.initialTime);
        putI64LE(recordBody.data() + 20, record.endTime);
        putI64LE(recordBody.data() + 28, record.receivedBeginTime);
        putI64LE(recordBody.data() + 36, record.receivedEndTime);
        putU32LE(recordBody.data() + 44, static_cast<uint32_t>(record.compressedBytes.size()));
        std::copy(record.compressedBytes.begin(), record.compressedBytes.end(), recordBody.begin() + 48);
        const auto frame = makeFrame(FrameType::DataRecord, recordBody);
        if (frame.empty() || !sendAll(client, frame.data(), frame.size())) return false;
    }
    const auto end = makeFrame(FrameType::BatchEnd, {kProtocolVersion});
    return !end.empty() && sendAll(client, end.data(), end.size());
}

bool TabletServer::handleLive(SOCKET client, const std::vector<uint8_t>& requestBody) {
    if (requestBody.size() != kLiveBodySize || requestBody[0] != kProtocolVersion ||
        (requestBody[1] != 0 && requestBody[1] != 1)) return sendError(client, "malformed live request");
    ReadDb reader(databasePath_, binaryPath_);
    if (!requestBody[1]) {
        ReadResult backfill = reader.readSince(getI64LE(requestBody.data() + 4));
        if (!backfill.ok()) return sendError(client, backfill.error);
        for (const Record& record : backfill.records) {
            std::vector<uint8_t> recordBody(48 + record.compressedBytes.size());
            recordBody[0] = kProtocolVersion;
            recordBody[1] = record.streamId;
            putI64LE(recordBody.data() + 4, record.id);
            putI64LE(recordBody.data() + 12, record.initialTime);
            putI64LE(recordBody.data() + 20, record.endTime);
            putI64LE(recordBody.data() + 28, record.receivedBeginTime);
            putI64LE(recordBody.data() + 36, record.receivedEndTime);
            putU32LE(recordBody.data() + 44, static_cast<uint32_t>(record.compressedBytes.size()));
            std::copy(record.compressedBytes.begin(), record.compressedBytes.end(), recordBody.begin() + 48);
            const auto frame = makeFrame(FrameType::DataRecord, recordBody);
            if (frame.empty() || !sendAll(client, frame.data(), frame.size())) return false;
        }
    }
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
            if (request.size() == 2 && request[0] == static_cast<uint8_t>(FrameType::Stop) &&
                request[1] == kProtocolVersion) return false;
            return sendError(client, "only stop is valid during live mode");
        }
        QueuedPacket queued;
        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            if (liveQueue_.empty()) continue;
            queued = std::move(liveQueue_.front());
            liveQueue_.pop_front();
        }
        std::vector<uint8_t> packetBody(24 + queued.packet.size());
        packetBody[0] = kProtocolVersion;
        packetBody[1] = queued.streamId;
        putI64LE(packetBody.data() + 4, queued.receivedBeginTimeMs);
        putI64LE(packetBody.data() + 12, queued.receivedEndTimeMs);
        putU32LE(packetBody.data() + 20, static_cast<uint32_t>(queued.packet.size()));
        std::copy(queued.packet.begin(), queued.packet.end(), packetBody.begin() + 24);
        const auto frame = makeFrame(FrameType::LivePacket, packetBody);
        if (frame.empty() || !sendAll(client, frame.data(), frame.size())) return false;
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
