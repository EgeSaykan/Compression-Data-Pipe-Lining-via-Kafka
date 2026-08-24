// receiver.cpp
//
// Windows Bluetooth SPP server.
// The PC advertises the standard Serial Port Profile UUID:
//
//   00001101-0000-1000-8000-00805F9B34FB
//
// The Android phone connects to this service and streams green/pink
// sensor batches using the packet layout documented in wire_protocol.h.
//
// This program does ONE job: read packets off the Bluetooth socket as
// fast as possible and hand each one to Kafka. It does NOT decompress,
// write CSV/binary files, or touch a database anymore -- that work
// moved to kafka_db_writer.cpp, which consumes from Kafka on two
// independent threads (one per stream) so a slow write on one stream
// can never block reads for the other, and neither can ever block the
// Bluetooth socket itself.
//
// Kafka production is asynchronous (produce() just enqueues), so even
// a temporarily slow/unavailable broker doesn't stall recv().
//
// Build deps (vcpkg): librdkafka
//   vcpkg install librdkafka

#include <winsock2.h>
#include <ws2bth.h>
#include <windows.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <string>
#include <vector>
#include <memory>

#include "wire_protocol.h"
#include <librdkafka/rdkafkacpp.h>

#pragma comment(lib, "Ws2_32.lib")

using phonepipe::PacketHeader;
using phonepipe::decodeHeader;
using phonepipe::kHeaderSize;
using phonepipe::streamName;
using phonepipe::kafkaTopicFor;

namespace {

//
// Standard Bluetooth Serial Port Profile UUID
//
const GUID SPP_UUID = {
    0x00001101,
    0x0000,
    0x1000,
    {0x80, 0x00, 0x00, 0x80, 0x5F, 0x9B, 0x34, 0xFB}
};

// ---------------------------------------------------------------------------
// Kafka producer: one shared producer instance, async, keyed by stream name
// so ordering is preserved per-stream. produce() only enqueues onto an
// internal per-topic queue and returns immediately; poll(0) below just
// services delivery-report callbacks without blocking on I/O.
// ---------------------------------------------------------------------------
class KafkaBatchProducer {
public:
    explicit KafkaBatchProducer(const std::string& brokers) {
        std::string errstr;

        std::unique_ptr<RdKafka::Conf> conf(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
        conf->set("bootstrap.servers", brokers, errstr);
        conf->set("queue.buffering.max.ms", "5", errstr);       // low-latency batching
        conf->set("queue.buffering.max.kbytes", "65536", errstr); // absorb bursts w/o blocking
        conf->set("dr_cb", &deliveryReportCb_, errstr);

        producer_.reset(RdKafka::Producer::create(conf.get(), errstr));
        if (!producer_) {
            fprintf(stderr, "Failed to create Kafka producer: %s\n", errstr.c_str());
        }
    }

    bool ok() const { return producer_ != nullptr; }

    // Hands the raw wire packet (header + payload, exactly as read from the
    // socket) to Kafka. Non-blocking. The consumer re-parses the header.
    void publish(uint8_t streamId, std::vector<uint8_t>&& rawPacket) {
        if (!producer_) return;

        const std::string topic = kafkaTopicFor(streamId);
        const std::string key = streamName(streamId);

        // produce() takes ownership of rawPacket's buffer via RK_MSG_COPY-free
        // path is avoided here for simplicity/safety; we copy once into the
        // librdkafka-managed buffer (RdKafka::Producer::RK_MSG_COPY).
        RdKafka::ErrorCode err = producer_->produce(
            topic,
            RdKafka::Topic::PARTITION_UA,
            RdKafka::Producer::RK_MSG_COPY,
            rawPacket.data(), rawPacket.size(),
            key.data(), key.size(),
            0 /* timestamp: let broker assign */,
            nullptr /* headers */,
            nullptr /* opaque */
        );

        if (err != RdKafka::ERR_NO_ERROR) {
            fprintf(stderr, "Kafka produce failed [%s]: %s\n",
                    topic.c_str(), RdKafka::err2str(err).c_str());
        }

        // Service delivery callbacks without blocking; 0ms timeout.
        producer_->poll(0);
    }

    void flush(int timeoutMs) {
        if (producer_) producer_->flush(timeoutMs);
    }

private:
    class DrCb : public RdKafka::DeliveryReportCb {
        void dr_cb(RdKafka::Message& msg) override {
            if (msg.err() != RdKafka::ERR_NO_ERROR) {
                fprintf(stderr, "Kafka delivery failed: %s\n", msg.errstr().c_str());
            }
        }
    };

    DrCb deliveryReportCb_;
    std::unique_ptr<RdKafka::Producer> producer_;
};

//
// Receive exactly n bytes from the Bluetooth socket.
//
bool readExact(SOCKET sock, void* buffer, size_t n) {
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
bool registerSppService(SOCKET sock, int port, HANDLE& serviceHandle) {
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
SOCKET createBluetoothServer() {
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

    HANDLE serviceHandle = nullptr;
    if (!registerSppService(sock, boundAddr.port, serviceHandle)) {
        closesocket(sock);
        return INVALID_SOCKET;
    }

    printf("Bluetooth SPP server listening on RFCOMM channel %d\n", boundAddr.port);
    printf("Waiting for Android phone...\n");

    return sock;
}

} // namespace

int main() {
    WSADATA wsaData{};
    int result = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (result != 0) {
        fprintf(stderr, "WSAStartup failed: %d\n", result);
        return 1;
    }

    SOCKET server = createBluetoothServer();
    if (server == INVALID_SOCKET) {
        WSACleanup();
        return 1;
    }

    SOCKADDR_BTH clientAddr{};
    int clientAddrLen = sizeof(clientAddr);
    SOCKET client = accept(server, reinterpret_cast<SOCKADDR*>(&clientAddr), &clientAddrLen);
    if (client == INVALID_SOCKET) {
        fprintf(stderr, "accept() failed: %d\n", WSAGetLastError());
        closesocket(server);
        WSACleanup();
        return 1;
    }

    printf("Phone connected!\n");

    // Kafka broker list; override with KAFKA_BROKERS env var if set.
    std::string brokers = "localhost:9092";
    if (const char* envBrokers = std::getenv("KAFKA_BROKERS")) {
        brokers = envBrokers;
    }

    KafkaBatchProducer producer(brokers);
    if (!producer.ok()) {
        closesocket(client);
        closesocket(server);
        WSACleanup();
        return 1;
    }

    //
    // Receive packets forever. This loop does nothing but read + publish,
    // so it never waits on decompression, disk I/O, or the database --
    // that's kafka_db_writer.cpp's job, running as a separate process
    // with one thread per stream.
    //
    while (true) {
        uint8_t header[phonepipe::kHeaderSize];

        if (!readExact(client, header, sizeof(header))) {
            fprintf(stderr, "Connection closed / read error\n");
            break;
        }

        const PacketHeader hdr = decodeHeader(header);

         printf("--- DEBUG: packet header ---\n");
         printf("compressed:    %s\n", hdr.compressed ? "true" : "false");
         printf("streamId:      %u (%s)\n",
             static_cast<unsigned>(hdr.streamId), streamName(hdr.streamId));
         printf("rowCount:      %u\n", hdr.rowCount);
         printf("payloadLen:    %u\n", hdr.payloadLen);
         printf("initialTimeMs: %lld\n",
             static_cast<long long>(hdr.initialTimeMs));
         printf("endTimeMs:     %lld\n",
             static_cast<long long>(hdr.endTimeMs));    
         printf("----------------------------\n");

        std::vector<uint8_t> rawPacket(phonepipe::kHeaderSize + hdr.payloadLen);
        std::memcpy(rawPacket.data(), header, phonepipe::kHeaderSize);

        if (!readExact(client, rawPacket.data() + phonepipe::kHeaderSize, hdr.payloadLen)) {
            fprintf(stderr, "Short payload read\n");
            break;
        }

        printf("[%s] %u rows, %u bytes on wire, t=[%lld..%lld] -> Kafka\n",
               streamName(hdr.streamId), hdr.rowCount, hdr.payloadLen,
               static_cast<long long>(hdr.initialTimeMs),
               static_cast<long long>(hdr.endTimeMs));

        producer.publish(hdr.streamId, std::move(rawPacket));
    }

    producer.flush(5000);

    closesocket(client);
    closesocket(server);
    WSACleanup();

    return 0;
}