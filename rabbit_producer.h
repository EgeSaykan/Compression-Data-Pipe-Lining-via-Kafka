#pragma once

#ifndef AMQP_STATIC
#define AMQP_STATIC
#endif
#include <rabbitmq-c/amqp.h>

#include <cstdint>
#include <string>
#include <vector>

bool checkRabbit(amqp_rpc_reply_t reply, const char* operation);

class RabbitBatchProducer {
public:
    RabbitBatchProducer();
    ~RabbitBatchProducer();

    bool ok() const;
    bool publish(uint8_t streamId, const std::vector<uint8_t>& packet);

private:
    amqp_connection_state_t connection_ = nullptr;
    amqp_socket_t* socket_ = nullptr;
    amqp_channel_t channel_ = 1;
    std::string exchange_;
    bool ready_ = false;
};