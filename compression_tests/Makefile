CXX := g++

COMMON_CXXFLAGS := -std=c++17 -O2 -pthread -I../openzl/include -I/ucrt64/include
COMMON_LDFLAGS := -L../openzl/build -L/ucrt64/lib -static
COMMON_LIBS := -lopenzl -lz -lzstd -llz4 -lws2_32 -lsecur32 -lcrypt32 -liphlpapi

.PHONY: all generate benchmark clean

all: generate_compression_inputs.exe compression_benchmark.exe

generate_compression_inputs.exe: generate_compression_inputs.cpp
	$(CXX) $(COMMON_CXXFLAGS) generate_compression_inputs.cpp -o $@

compression_benchmark.exe: compression_benchmark.cpp ../openzl_codec.cpp ../openzl_codec.h ../wire_protocol.h
	$(CXX) $(COMMON_CXXFLAGS) $(COMMON_LDFLAGS) compression_benchmark.cpp ../openzl_codec.cpp -o $@ $(COMMON_LIBS)

generate: generate_compression_inputs.exe
	./generate_compression_inputs.exe inputs

benchmark: compression_benchmark.exe
	./compression_benchmark.exe inputs results > results.csv

clean:
	rm -f generate_compression_inputs.exe compression_benchmark.exe results.csv
	rm -rf inputs results