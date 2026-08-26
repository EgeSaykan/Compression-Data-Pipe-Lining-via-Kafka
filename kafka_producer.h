#pragma once

#include <librdkafka/rdkafkacpp.h>

#include <memory>
#include <string>
#include <vector>

class KafkaBatchProducer {
public:
    explicit KafkaBatchProducer(const std::string& brokers);

    bool ok() const;
    void publish(uint8_t streamId, std::vector<uint8_t>&& rawPacket);
    void flush(int timeoutMs);

private:
    class DrCb : public RdKafka::DeliveryReportCb {
    public:
        void dr_cb(RdKafka::Message& msg) override;
    };

    DrCb deliveryReportCb_;
    std::unique_ptr<RdKafka::Producer> producer_;
};
