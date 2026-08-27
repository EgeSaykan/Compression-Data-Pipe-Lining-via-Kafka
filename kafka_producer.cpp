
#include <librdkafka/rdkafkacpp.h>

#include <cstdio>
#include <string>

#include "kafka_producer.h"
#include "wire_protocol.h"


using phonepipe::kafkaTopicFor;
using phonepipe::streamName;

KafkaBatchProducer::KafkaBatchProducer(const std::string& brokers) {
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

bool KafkaBatchProducer::ok() const {
    return producer_ != nullptr;
}

    // Hands the raw wire packet (header + payload, exactly as read from the
    // socket) to Kafka. Non-blocking. The consumer re-parses the header.
void KafkaBatchProducer::publish(uint8_t streamId, std::vector<uint8_t>&& rawPacket) {
        if (!producer_) return;

        const std::string topic = kafkaTopicFor(streamId);
        const std::string key = std::to_string(streamId);

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

void KafkaBatchProducer::flush(int timeoutMs) {
        if (producer_) producer_->flush(timeoutMs);
}

void KafkaBatchProducer::DrCb::dr_cb(RdKafka::Message& msg) {
    if (msg.err() != RdKafka::ERR_NO_ERROR) {
        fprintf(stderr, "Kafka delivery failed: %s\n", msg.errstr().c_str());
    }
}