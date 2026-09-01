#include <winsock2.h>
#include <ws2bth.h>
#include <windows.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

#include "bluetooth_receiver.h"
#include "wire_protocol.h"
#include "gzip_codec.h"
#include "openzl_codec.h"


#pragma comment(lib, "Ws2_32.lib")


namespace phonepipe {

namespace {

int64_t utcNowMs() {
    FILETIME fileTime{};
    GetSystemTimeAsFileTime(&fileTime);
    const uint64_t windowsTime = (static_cast<uint64_t>(fileTime.dwHighDateTime) << 32) |
                                 fileTime.dwLowDateTime;
    return static_cast<int64_t>(windowsTime / 10000ULL - 11644473600000ULL);
}

// Rewrites `packet` in place so its flag becomes OPENZL, converting RAW and
// GZIP payloads to OpenZL-compressed form before anything downstream (tablet
// server / Kafka / RabbitMQ) ever sees them. Already-OPENZL packets pass
// through untouched. On failure, leaves `packet` as-is and logs to stderr.
void normalizeToOpenZL(std::vector<uint8_t>& packet) {
    if (packet.size() < kHeaderSize) return;
    PacketHeader hdr = decodeHeader(packet.data());
    if (hdr.flag == CompressionFlag::OPENZL) return;

    const uint8_t* payload = packet.data() + kHeaderSize;
    std::vector<uint8_t> compressed;

    if (hdr.flag == CompressionFlag::GZIP) {
        std::vector<uint8_t> raw = gzipDecompress(payload, hdr.payloadLen);
        if (raw.empty()) {
            fprintf(stderr, "normalizeToOpenZL: gzip decompress failed for stream %u\n", hdr.streamId);
            return;
        }
        compressed = openzlCompressFresh(raw.data(), raw.size(), static_cast<int>(hdr.rowCount));
    } else { // RAW -- already delta-encoded on-device, entropy-compress only
        compressed = openzlCompressDeltaEncoded(payload, hdr.payloadLen, static_cast<int>(hdr.rowCount));
    }

    if (compressed.empty()) {
        fprintf(stderr, "normalizeToOpenZL: OpenZL compression failed for stream %u, forwarding as-is\n",
                hdr.streamId);
        return;
    }

    hdr.flag = CompressionFlag::OPENZL;
    hdr.payloadLen = static_cast<uint32_t>(compressed.size());

    std::vector<uint8_t> rebuilt(kHeaderSize + compressed.size());
    encodeHeader(rebuilt.data(), hdr);
    std::memcpy(rebuilt.data() + kHeaderSize, compressed.data(), compressed.size());
    packet = std::move(rebuilt);
}

}

const GUID SPP_UUID = {
    0x00001101, 0x0000, 0x1000,
    {0x80, 0x00, 0x00, 0x80, 0x5F, 0x9B, 0x34, 0xFB}
};

//
// Receive exactly n bytes from the Bluetooth socket.
//
bool BluetoothReceiver::readExact(SOCKET sock, void* buffer, size_t n) {
    auto* p = static_cast<uint8_t*>(buffer);
    size_t received = 0;

    while (received < n) {
        int r = recv(sock, reinterpret_cast<char*>(p + received),
                      static_cast<int>(n - received), 0);
        if (r <= 0) return false;
        received += static_cast<size_t>(r);
    }
    return true;
}

//
// Register the SPP service with Windows Bluetooth SDP.
//
bool BluetoothReceiver::registerSppService(int port) {
    SOCKADDR_BTH localAddr{};
    localAddr.addressFamily = AF_BTH;
    localAddr.btAddr = 0;
    localAddr.serviceClassId = SPP_UUID;
    localAddr.port = port;

    CSADDR_INFO csAddr{};
    csAddr.iProtocol = BTHPROTO_RFCOMM;
    csAddr.iSocketType = SOCK_STREAM;
    csAddr.LocalAddr.lpSockaddr = reinterpret_cast<LPSOCKADDR>(&localAddr);
    csAddr.LocalAddr.iSockaddrLength = sizeof(localAddr);

    WSAQUERYSETW query{};
    query.dwSize = sizeof(query);
    query.dwNameSpace = NS_BTH;
    query.lpServiceClassId = const_cast<LPGUID>(&SPP_UUID);
    query.lpszServiceInstanceName = const_cast<LPWSTR>(L"Ege Sensor SPP");
    query.dwNumberOfCsAddrs = 1;
    query.lpcsaBuffer = &csAddr;

    int result = WSASetServiceW(&query, RNRSERVICE_REGISTER, 0);
    if (result != 0) {
        printf("WSASetService failed: %d\n", WSAGetLastError());
        return false;
    }
    return true;
}

//
// Create Bluetooth RFCOMM listening socket.
//
SOCKET BluetoothReceiver::createBluetoothServer() {
    SOCKET sock = socket(AF_BTH, SOCK_STREAM, BTHPROTO_RFCOMM);
    if (sock == INVALID_SOCKET) {
        printf("socket() failed: %d\n", WSAGetLastError());
        return INVALID_SOCKET;
    }

    SOCKADDR_BTH addr{};
    addr.addressFamily = AF_BTH;
    addr.btAddr = 0;               // this Bluetooth adapter
    addr.serviceClassId = SPP_UUID;
    addr.port = BT_PORT_ANY;       // let Windows choose an RFCOMM channel

    if (bind(sock, reinterpret_cast<SOCKADDR*>(&addr), sizeof(addr)) == SOCKET_ERROR) {
        printf("bind() failed: %d\n", WSAGetLastError());
        closesocket(sock);
        return INVALID_SOCKET;
    }

    SOCKADDR_BTH boundAddr{};
    int addrLen = sizeof(boundAddr);
    if (getsockname(sock, reinterpret_cast<SOCKADDR*>(&boundAddr), &addrLen) == SOCKET_ERROR) {
        printf("getsockname() failed: %d\n", WSAGetLastError());
        closesocket(sock);
        return INVALID_SOCKET;
    }

    if (listen(sock, 1) == SOCKET_ERROR) {
        printf("listen() failed: %d\n", WSAGetLastError());
        closesocket(sock);
        return INVALID_SOCKET;
    }

    if (!registerSppService(boundAddr.port)) {
        closesocket(sock);
        return INVALID_SOCKET;
    }

    printf("Bluetooth SPP server listening on RFCOMM channel %d\n", boundAddr.port);
    printf("Waiting for Android phone...\n");

    return sock;
}

BluetoothReceiver::BluetoothReceiver() {
    WSADATA wsaData{};
    const int result = WSAStartup(MAKEWORD(2, 2), &wsaData);
    wsaStarted_ = result == 0;
    if (!wsaStarted_) {
        fprintf(stderr, "WSAStartup failed: %d\n", result);
        return;
    }

    server_ = createBluetoothServer();
}

BluetoothReceiver::~BluetoothReceiver() {
    if (client_ != INVALID_SOCKET) closesocket(client_);
    if (server_ != INVALID_SOCKET) closesocket(server_);
    if (wsaStarted_) WSACleanup();
}

bool BluetoothReceiver::ok() const {
    return wsaStarted_ && server_ != INVALID_SOCKET;
}

void BluetoothReceiver::run(const PacketHandler& handler) {
    if (!ok()) return;

    while (true) {
        SOCKADDR_BTH clientAddr{};
        int clientLength = sizeof(clientAddr);
        client_ = accept(server_, reinterpret_cast<SOCKADDR*>(&clientAddr), &clientLength);
        if (client_ == INVALID_SOCKET) {
            fprintf(stderr, "accept() failed: %d\n", WSAGetLastError());
            return;
        }
        printf("Phone connected!\n");

        bool connected = true;
        while (connected) {
            const int64_t receivedBeginTimeMs = utcNowMs();
            uint8_t header[kHeaderSize];
            if (!readExact(client_, header, sizeof(header))) {
                fprintf(stderr, "Phone disconnected or Bluetooth read failed\n");
                connected = false;
                continue;
            }

            const PacketHeader packetHeader = decodeHeader(header);
            std::vector<uint8_t> packet(kHeaderSize + packetHeader.payloadLen);
            std::memcpy(packet.data(), header, kHeaderSize);
            if (!readExact(client_, packet.data() + kHeaderSize, packetHeader.payloadLen)) {
                fprintf(stderr, "Phone disconnected during payload read\n");
                connected = false;
                continue;
            }
            const int64_t receivedEndTimeMs = utcNowMs();

            if (packetHeader.streamId != STREAM_GREEN && packetHeader.streamId != STREAM_PINK) {
                fprintf(stderr, "Ignoring packet with unknown stream %u\n", packetHeader.streamId);
                continue;
            }

            // Normalize RAW/GZIP -> OPENZL before this packet goes anywhere
            // else (tablet server, Kafka, RabbitMQ all expect OpenZL from
            // here on). Must happen before handler(...) below.
            normalizeToOpenZL(packet);
            const PacketHeader finalHeader = decodeHeader(packet.data());

            printf("[%s] %u rows, %u bytes (%s -> %s)\n", streamName(finalHeader.streamId),
                   finalHeader.rowCount, finalHeader.payloadLen,
                   compressionName(packetHeader.flag), compressionName(finalHeader.flag));
            handler(finalHeader.streamId, std::move(packet),
                receivedBeginTimeMs, receivedEndTimeMs);
        }

        closesocket(client_);
        client_ = INVALID_SOCKET;
    }
}

} // namespace phonepipe
