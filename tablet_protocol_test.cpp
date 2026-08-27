#include "tablet_protocol.h"

#include <cassert>

int main() {
    uint8_t bytes[8]{};
    tabletpipe::putU64LE(bytes, 0x8877665544332211ULL);
    assert(tabletpipe::getU64LE(bytes) == 0x8877665544332211ULL);
    tabletpipe::putI64LE(bytes, -123456789);
    assert(tabletpipe::getI64LE(bytes) == -123456789);

    const std::vector<uint8_t> body{1, 2, 3, 4};
    const auto frame = tabletpipe::makeFrame(tabletpipe::FrameType::LivePacket, body);
    assert(frame.size() == 9);
    assert(tabletpipe::getU32LE(frame.data()) == 5);
    assert(frame[4] == static_cast<uint8_t>(tabletpipe::FrameType::LivePacket));
    assert(frame[5] == 1 && frame[8] == 4);
    assert(tabletpipe::parseLength(1));
    assert(tabletpipe::parseLength(tabletpipe::kMaxFrameSize));
    assert(!tabletpipe::parseLength(0));
    assert(!tabletpipe::parseLength(tabletpipe::kMaxFrameSize + 1));
    return 0;
}
