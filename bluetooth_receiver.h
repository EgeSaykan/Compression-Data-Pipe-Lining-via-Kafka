#pragma once

#include <winsock2.h>
#include <ws2bth.h>
#include <windows.h>

#include <cstdint>
#include <functional>
#include <vector>

namespace phonepipe {

using PacketHandler = std::function<void(uint8_t, std::vector<uint8_t>&&)>;

class BluetoothReceiver {
public:
    BluetoothReceiver();
    ~BluetoothReceiver();

    BluetoothReceiver(const BluetoothReceiver&) = delete;
    BluetoothReceiver& operator=(const BluetoothReceiver&) = delete;

    bool ok() const;
    void run(const PacketHandler& handler);

private:
    static bool readExact(SOCKET socket, void* buffer, size_t size);
    static bool registerSppService(int port);
    static SOCKET createBluetoothServer();

    SOCKET server_ = INVALID_SOCKET;
    SOCKET client_ = INVALID_SOCKET;
    bool wsaStarted_ = false;
};

} // namespace phonepipe
