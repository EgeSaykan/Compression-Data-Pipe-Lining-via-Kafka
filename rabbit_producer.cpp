
#ifndef AMQP_STATIC
#define AMQP_STATIC
#endif
#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>

#include <cstdio>
#include <cstdlib>
#include <string>

#include "rabbit_producer.h"
#include "wire_protocol.h"


const char* envOr(const char* name, const char* fallback) {
    const char* value = std::getenv(name);
    return value && *value ? value : fallback;
}

bool checkRabbit(amqp_rpc_reply_t reply, const char* operation) {
    if (reply.reply_type == AMQP_RESPONSE_NORMAL) return true;
    fprintf(stderr, "RabbitMQ %s failed (reply type %d)\n", operation, reply.reply_type);
    return false;
}

RabbitBatchProducer::RabbitBatchProducer() {
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

RabbitBatchProducer::~RabbitBatchProducer() {
        if (connection_) {
            if (ready_) amqp_channel_close(connection_, channel_, AMQP_REPLY_SUCCESS);
            amqp_connection_close(connection_, AMQP_REPLY_SUCCESS);
            amqp_destroy_connection(connection_);
        }
}

bool RabbitBatchProducer::ok() const {
    return ready_;
}

bool RabbitBatchProducer::publish(uint8_t streamId, const std::vector<uint8_t>& packet) {
        const std::string key = phonepipe::streamName(streamId);
        amqp_bytes_t body{
            packet.size(),
            const_cast<uint8_t*>(packet.data())
        };
        amqp_basic_properties_t props{};
        props._flags = AMQP_BASIC_DELIVERY_MODE_FLAG | AMQP_BASIC_PRIORITY_FLAG;
        props.delivery_mode = 2; // survive broker restart when queues are durable
        props.priority = streamId == phonepipe::STREAM_PINK ? 10 : 1;
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