// Bluetooth SPP receiver that publishes complete sensor packets to RabbitMQ.
// Build dependency: rabbitmq-c (amqp.h) and Ws2_32.lib.

#include <winsock2.h>
#include <ws2bth.h>
#include <windows.h>

#ifndef AMQP_STATIC
#define AMQP_STATIC
#endif
#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "wire_protocol.h"

#pragma comment(lib, "Ws2_32.lib")

using namespace phonepipe;

namespace {

const GUID SPP_UUID = {
    0x00001101, 0x0000, 0x1000,
    {0x80, 0x00, 0x00, 0x80, 0x5F, 0x9B, 0x34, 0xFB}
};

const char* envOr(const char* name, const char* fallback) {
    const char* value = std::getenv(name);
    return value && *value ? value : fallback;
}

bool readExact(SOCKET sock, void* buffer, size_t size) {
    auto* bytes = static_cast<uint8_t*>(buffer);
    size_t received = 0;
    while (received < size) {
        int count = recv(sock, reinterpret_cast<char*>(bytes + received),
                         static_cast<int>(size - received), 0);
        if (count <= 0) return false;
        received += static_cast<size_t>(count);
    }
    return true;
}

bool checkRabbit(amqp_rpc_reply_t reply, const char* operation) {
    if (reply.reply_type == AMQP_RESPONSE_NORMAL) return true;
    fprintf(stderr, "RabbitMQ %s failed (reply type %d)\n", operation, reply.reply_type);
    return false;
}

class RabbitBatchProducer {
public:
    RabbitBatchProducer() {
        connection_ = amqp_new_connection();
        socket_ = amqp_tcp_socket_new(connection_);
        if (!socket_) return;

        const int port = std::atoi(envOr("RABBITMQ_PORT", "5672"));
        if (amqp_socket_open(socket_, envOr("RABBITMQ_HOST", "localhost"), port)) return;

        amqp_rpc_reply_t reply = amqp_login(
            connection_, envOr("RABBITMQ_VHOST", "/"), 0, 131072, 0,
            AMQP_SASL_METHOD_PLAIN,
            envOr("RABBITMQ_USER", "socar"), envOr("RABBITMQ_PASSWORD", "socar-dev-password"));
        if (!checkRabbit(reply, "login")) return;

        amqp_channel_open(connection_, channel_);
        if (!checkRabbit(amqp_get_rpc_reply(connection_), "channel open")) return;

        const std::string exchange = envOr("RABBITMQ_EXCHANGE", "phonepipe");
        amqp_exchange_declare(connection_, channel_, amqp_cstring_bytes(exchange.c_str()),
                              amqp_cstring_bytes("direct"), 0, 1, 0, 0, amqp_empty_table);
        if (!checkRabbit(amqp_get_rpc_reply(connection_), "exchange declare")) return;

        amqp_table_entry_t priorityEntry{};
        priorityEntry.key = amqp_cstring_bytes("x-max-priority");
        priorityEntry.value.kind = AMQP_FIELD_KIND_I32;
        priorityEntry.value.value.i32 = 10;
        amqp_table_t queueArguments{};
        queueArguments.num_entries = 1;
        queueArguments.entries = &priorityEntry;
        amqp_queue_declare(connection_, channel_, amqp_cstring_bytes("phonepipe-db"),
                           0, 1, 0, 0, queueArguments);
        if (!checkRabbit(amqp_get_rpc_reply(connection_), "queue declare")) return;
        for (const char* routingKey : {"pink", "green"}) {
            amqp_queue_bind(connection_, channel_, amqp_cstring_bytes("phonepipe-db"),
                            amqp_cstring_bytes(exchange.c_str()), amqp_cstring_bytes(routingKey),
                            amqp_empty_table);
            if (!checkRabbit(amqp_get_rpc_reply(connection_), "queue bind")) return;
        }
        exchange_ = exchange;
        ready_ = true;
    }

    ~RabbitBatchProducer() {
        if (connection_) {
            if (ready_) amqp_channel_close(connection_, channel_, AMQP_REPLY_SUCCESS);
            amqp_connection_close(connection_, AMQP_REPLY_SUCCESS);
            amqp_destroy_connection(connection_);
        }
    }

    bool ok() const { return ready_; }

    bool publish(uint8_t streamId, const std::vector<uint8_t>& packet) {
        const std::string key = streamName(streamId);
        amqp_bytes_t body{
            packet.size(),
            const_cast<uint8_t*>(packet.data())
        };
        amqp_basic_properties_t props{};
        props._flags = AMQP_BASIC_DELIVERY_MODE_FLAG | AMQP_BASIC_PRIORITY_FLAG;
        props.delivery_mode = 2; // survive broker restart when queues are durable
        props.priority = streamId == STREAM_PINK ? 10 : 1;
        const int result = amqp_basic_publish(
            connection_, channel_, amqp_cstring_bytes(exchange_.c_str()),
            amqp_cstring_bytes(key.c_str()), 0, 0, &props,
            body);
        if (result != AMQP_STATUS_OK) {
            fprintf(stderr, "RabbitMQ publish failed for %s: %d\n", key.c_str(), result);
            return false;
        }
        return true;
    }

private:
    amqp_connection_state_t connection_ = nullptr;
    amqp_socket_t* socket_ = nullptr;
    amqp_channel_t channel_ = 1;
    std::string exchange_;
    bool ready_ = false;
};

bool registerSppService(int port) {
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

    if (!registerSppService(boundAddr.port)) {
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
        printf("server socket handle: %llu\n",
            static_cast<unsigned long long>(server));
    if (server == INVALID_SOCKET) {
        printf("Failed to create Bluetooth server: %d\n", WSAGetLastError());
        WSACleanup();
        return 1;
    }

    RabbitBatchProducer producer;
    if (!producer.ok()) {
        printf("Failed to connect to RabbitMQ\n");
        closesocket(server);
        WSACleanup();
        return 1;
    }

    printf("Waiting for Android phone...\n");
    SOCKADDR_BTH clientAddr{};
    int clientLength = sizeof(clientAddr);
    SOCKET client = accept(server, reinterpret_cast<SOCKADDR*>(&clientAddr), &clientLength);
    printf("client socket: %llu\n", static_cast<unsigned long long>(client));
    if (client == INVALID_SOCKET) {
        fprintf(stderr, "accept() failed: %d\n", WSAGetLastError());
        closesocket(server);
        WSACleanup();
        return 1;
    }
    printf("Phone connected!\n");

    while (true) {
        uint8_t header[kHeaderSize];
        if (!readExact(client, header, sizeof(header))) {
            fprintf(stderr, "Phone disconnected or Bluetooth read failed\n");
            break;
        }
        const PacketHeader packetHeader = decodeHeader(header);
        std::vector<uint8_t> packet(kHeaderSize + packetHeader.payloadLen);
        std::memcpy(packet.data(), header, kHeaderSize);
        if (!readExact(client, packet.data() + kHeaderSize, packetHeader.payloadLen)) {
            fprintf(stderr, "Phone disconnected during payload read\n");
            break;
        }

        if (packetHeader.streamId != STREAM_GREEN && packetHeader.streamId != STREAM_PINK) {
            fprintf(stderr, "Ignoring packet with unknown stream %u\n", packetHeader.streamId);
            continue;
        }
        printf("[%s] %u rows, %u bytes -> RabbitMQ\n", streamName(packetHeader.streamId),
               packetHeader.rowCount, packetHeader.payloadLen);
        producer.publish(packetHeader.streamId, packet);
    }

    closesocket(client);
    closesocket(server);
    WSACleanup();
    return 0;
}