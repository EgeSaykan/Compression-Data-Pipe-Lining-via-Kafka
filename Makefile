CXX := g++
CC := gcc

COMMON_CXXFLAGS := -std=c++17 -O2 -pthread -Iopenzl/include -I/ucrt64/include
COMMON_LDFLAGS := -Lopenzl/build -L/ucrt64/lib -static
COMMON_CRYPTO_LIBS := -lssl -lcrypto -lz -lzstd -llz4
WINDOWS_LIBS := -lws2_32 -lsecur32 -lcrypt32 -liphlpapi

LIBRDKAFKA_INCLUDE := -I"D:/Safe/Durham/SOCAR/Project/librdkafka/install/include"
LIBRDKAFKA_LIBRARY := -L"D:/Safe/Durham/SOCAR/Project/librdkafka/install/lib"
LIBRDKAFKA_LIBS := -Wl,--start-group -lrdkafka++ -lrdkafka -Wl,--end-group
RABBITMQ_LIBS := -Wl,--start-group -lrabbitmq -Wl,--end-group
OPENZL_LIBS := -lopenzl
SQLITE_LIBS := -lsqlite3

.PHONY: all clean

all: pipe_receiver.exe kafka_db_writer.exe rabbitmq_db_writer.exe

pipe_receiver.exe: pipe_receiver.cpp bluetooth_receiver.cpp bluetooth_receiver.h client_bluetooth.cpp client_bluetooth.h read_db.cpp read_db.h tablet_protocol.h kafka_producer.cpp kafka_producer.h rabbit_producer.cpp rabbit_producer.h wire_protocol.h gzip_codec.cpp gzip_codec.h openzl_codec.cpp openzl_codec.h
	$(CXX) $(COMMON_CXXFLAGS) $(LIBRDKAFKA_INCLUDE) $(LIBRDKAFKA_LIBRARY) $(COMMON_LDFLAGS) -DLIBRDKAFKA_STATICLIB \
		pipe_receiver.cpp bluetooth_receiver.cpp client_bluetooth.cpp read_db.cpp kafka_producer.cpp rabbit_producer.cpp gzip_codec.cpp openzl_codec.cpp -o $@ \
		$(LIBRDKAFKA_LIBS) $(RABBITMQ_LIBS) $(OPENZL_LIBS) $(COMMON_CRYPTO_LIBS) $(SQLITE_LIBS) $(WINDOWS_LIBS) -lbthprops

kafka_db_writer.exe: kafka_db_writer.cpp wire_protocol.h gzip_codec.cpp gzip_codec.h openzl_codec.cpp openzl_codec.h
	$(CXX) $(COMMON_CXXFLAGS) $(LIBRDKAFKA_INCLUDE) $(LIBRDKAFKA_LIBRARY) $(COMMON_LDFLAGS) -DLIBRDKAFKA_STATICLIB \
		kafka_db_writer.cpp gzip_codec.cpp openzl_codec.cpp -o $@ \
		$(LIBRDKAFKA_LIBS) $(OPENZL_LIBS) $(COMMON_CRYPTO_LIBS) $(SQLITE_LIBS) $(WINDOWS_LIBS)

rabbitmq_db_writer.exe: rabbitmq_db_writer.cpp wire_protocol.h gzip_codec.cpp gzip_codec.h openzl_codec.cpp openzl_codec.h
	$(CXX) $(COMMON_CXXFLAGS) $(COMMON_LDFLAGS) \
		rabbitmq_db_writer.cpp gzip_codec.cpp openzl_codec.cpp -o $@ \
		$(RABBITMQ_LIBS) $(OPENZL_LIBS) $(COMMON_CRYPTO_LIBS) $(SQLITE_LIBS) $(WINDOWS_LIBS)

read_db_test.exe: read_db.cpp read_db_test.cpp read_db.h sqlite3_test.o
	$(CXX) $(COMMON_CXXFLAGS) -I. read_db.cpp read_db_test.cpp sqlite3_test.o -o $@

sqlite3_test.o: sqlite3.c sqlite3.h
	$(CC) -I. -c sqlite3.c -o $@

tablet_protocol_test.exe: tablet_protocol_test.cpp tablet_protocol.h
	$(CXX) $(COMMON_CXXFLAGS) -I. tablet_protocol_test.cpp -o $@

test: read_db_test.exe tablet_protocol_test.exe
	./read_db_test.exe
	./tablet_protocol_test.exe

clean:
	rm -f pipe_receiver.exe kafka_db_writer.exe rabbitmq_db_writer.exe read_db_test.exe tablet_protocol_test.exe sqlite3_test.o read_db_test.sqlite3 read_db_test.bin