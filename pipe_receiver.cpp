// UNCHANGED from your original. Normalization (raw/gzip -> OpenZL) now
// happens once, inside BluetoothReceiver::run (see bluetooth_receiver.cpp),
// before the handler lambda below ever runs -- so every packet this file
// touches is already OpenZL-compressed regardless of what the phone sent.

#include <cstdio>
#include <cstdlib>
#include <cerrno>
#include <climits>
#include <thread>
#include <string>
#include <vector>

#include "bluetooth_receiver.h"
#include "client_bluetooth.h"
#include "rabbit_producer.h"
#include "kafka_producer.h"
#include "wire_protocol.h"


#pragma comment(lib, "Ws2_32.lib")

bool parseCopyCount(const char* text, uint32_t& count) {
	if (!text || !*text) return false;
	char* end = nullptr;
	errno = 0;
	const unsigned long value = std::strtoul(text, &end, 10);
	if (errno == ERANGE || *end != '\0' || value == 0 || value > 128) return false;
	count = static_cast<uint32_t>(value);
	return true;
}

int main(int argc, char* argv[]) {
	if (argc < 2 || argc > 3 ||
		(std::string(argv[1]) != "--kafka" && std::string(argv[1]) != "--rabbitmq")) {
		fprintf(stderr, "Usage: %s --kafka|--rabbitmq [N]\n", argv[0]);
		return 1;
	}

	uint32_t copyCount = 1;
	if (argc == 3 && !parseCopyCount(argv[2], copyCount)) {
		fprintf(stderr, "N must be an integer from 1 to 128\n");
		return 1;
	}

	phonepipe::BluetoothReceiver receiver;
	if (!receiver.ok()) return 1;
	tabletpipe::TabletServer tabletServer;
	if (!tabletServer.ok()) return 1;

	if (std::string(argv[1]) == "--kafka") {
		const char* configuredBrokers = std::getenv("KAFKA_BROKERS");
		KafkaBatchProducer producer(configuredBrokers ? configuredBrokers : "localhost:9092");
		if (!producer.ok()) { 
			printf("Docker probs");
			return 1;
		}

		std::thread tabletThread([&tabletServer] { tabletServer.run(); });
		receiver.run([&producer, &tabletServer, copyCount](uint8_t streamId, std::vector<uint8_t>&& packet,
                                                     int64_t receivedBeginTimeMs, int64_t receivedEndTimeMs) {
			for (uint32_t copyIndex = 0; copyIndex < copyCount; ++copyIndex) {
                std::vector<uint8_t> copy = packet;
				const uint8_t pipeId = phonepipe::virtualStreamId(streamId, copyIndex);
				copy[1] = pipeId;
				if (tabletServer.isLiveClientConnected() && packet.size() >= phonepipe::kHeaderSize) {
					const phonepipe::PacketHeader header = phonepipe::decodeHeader(packet.data());
					const size_t wireSize = phonepipe::kHeaderSize + header.payloadLen;
					if (packet.size() >= wireSize) {
						std::vector<uint8_t> compressedOnly(packet.begin() + phonepipe::kHeaderSize,
																		 packet.begin() + wireSize);
						tabletServer.offerLivePacket(pipeId, compressedOnly, receivedBeginTimeMs,
																	 receivedEndTimeMs, header.rowCount);
					}
				}
				phonepipe::appendReceivedTimes(copy, receivedBeginTimeMs, receivedEndTimeMs);
				producer.publish(copy[1], std::move(copy));
			}
		});
		producer.flush(5000);
		tabletServer.stop();
		tabletThread.join();
	} else {
		RabbitBatchProducer producer;
		printf("here1");
		if (!producer.ok()) { 
			printf("Docker probs");
			return 1;
		}
		printf("here2");

		std::thread tabletThread([&tabletServer] { tabletServer.run(); });
		receiver.run([&producer, &tabletServer, copyCount](uint8_t streamId, std::vector<uint8_t>&& packet,
                                                     int64_t receivedBeginTimeMs, int64_t receivedEndTimeMs) {
			for (uint32_t copyIndex = 0; copyIndex < copyCount; ++copyIndex) {
                std::vector<uint8_t> copy = packet;
				const uint8_t pipeId = phonepipe::virtualStreamId(streamId, copyIndex);
				copy[1] = pipeId;
				if (tabletServer.isLiveClientConnected() && packet.size() >= phonepipe::kHeaderSize) {
					const phonepipe::PacketHeader header = phonepipe::decodeHeader(packet.data());
					const size_t wireSize = phonepipe::kHeaderSize + header.payloadLen;
					if (packet.size() >= wireSize) {
						std::vector<uint8_t> compressedOnly(packet.begin() + phonepipe::kHeaderSize,
																		 packet.begin() + wireSize);
						tabletServer.offerLivePacket(pipeId, compressedOnly, receivedBeginTimeMs,
																	 receivedEndTimeMs, header.rowCount);
					}
				}
				phonepipe::appendReceivedTimes(copy, receivedBeginTimeMs, receivedEndTimeMs);
				producer.publish(copy[1], copy);
			}
		});
		tabletServer.stop();
		tabletThread.join();
	}

	return 0;
}
