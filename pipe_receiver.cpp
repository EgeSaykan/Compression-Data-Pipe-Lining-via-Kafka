



#include <cstdio>
#include <cstdlib>
#include <string>

#include "bluetooth_receiver.h"
#include "rabbit_producer.h"
#include "kafka_producer.h"


#pragma comment(lib, "Ws2_32.lib")

int main(int argc, char* argv[]) {
	if (argc != 2 || (std::string(argv[1]) != "--kafka" &&
					  std::string(argv[1]) != "--rabbitmq")) {
		fprintf(stderr, "Usage: %s --kafka | --rabbitmq\n", argv[0]);
		return 1;
	}

	phonepipe::BluetoothReceiver receiver;
	if (!receiver.ok()) return 1;

	if (std::string(argv[1]) == "--kafka") {
		const char* configuredBrokers = std::getenv("KAFKA_BROKERS");
		KafkaBatchProducer producer(configuredBrokers ? configuredBrokers : "localhost:9092");
		if (!producer.ok()) return 1;

		receiver.run([&producer](uint8_t streamId, std::vector<uint8_t>&& packet) {
			producer.publish(streamId, std::move(packet));
		});
		producer.flush(5000);
	} else {
		RabbitBatchProducer producer;
		if (!producer.ok()) return 1;

		receiver.run([&producer](uint8_t streamId, std::vector<uint8_t>&& packet) {
			producer.publish(streamId, packet);
		});
	}

	return 0;
}

