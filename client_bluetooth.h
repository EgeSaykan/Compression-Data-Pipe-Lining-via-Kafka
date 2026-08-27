#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <vector>

#include <winsock2.h>
#include <ws2bth.h>

namespace tabletpipe {

struct QueuedPacket {
    uint8_t streamId = 0;
    int64_t receivedBeginTimeMs = 0;
    int64_t receivedEndTimeMs = 0;
    std::vector<uint8_t> packet;
};

class TabletServer {
public:
    TabletServer(std::string databasePath = "data/phonepipe.sqlite3",
                 std::string binaryPath = "data/phonepipe.bin");
    ~TabletServer();

    TabletServer(const TabletServer&) = delete;
    TabletServer& operator=(const TabletServer&) = delete;

    bool ok() const;
    bool isLiveClientConnected() const;
    void offerLivePacket(uint8_t streamId, const std::vector<uint8_t>& packet,
                         int64_t receivedBeginTimeMs = 0,
                         int64_t receivedEndTimeMs = 0);
    void run();
    void stop();

private:
    bool registerSppService(int port);
    SOCKET createBluetoothServer();
    void serveClient(SOCKET client);
    bool handleRequest(SOCKET client, const std::vector<uint8_t>& payload);
    bool handleBatch(SOCKET client, const std::vector<uint8_t>& body);
    bool handleLive(SOCKET client, const std::vector<uint8_t>& body);
    bool sendAll(SOCKET socket, const uint8_t* data, size_t size);
    bool receiveFrame(SOCKET socket, std::vector<uint8_t>& payload, int timeoutMs);
    bool sendError(SOCKET client, const std::string& message);
    void clearLiveState();

    SOCKET server_ = INVALID_SOCKET;
    std::atomic<SOCKET> client_{INVALID_SOCKET};
    std::atomic<bool> liveClientConnected_{false};
    std::atomic<bool> liveModeActive_{false};
    std::atomic<bool> stopping_{false};
    bool wsaStarted_ = false;
    std::mutex queueMutex_;
    std::deque<QueuedPacket> liveQueue_;
    static constexpr size_t kLiveQueueCapacity = 256;
    std::string databasePath_;
    std::string binaryPath_;
};

} // namespace tabletpipe
