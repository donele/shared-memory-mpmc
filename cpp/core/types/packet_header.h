#pragma once

#include "time_types.h"

namespace sgt {

struct PacketHeader {
    uint16_t type = msg_type::PACKET_HEADER;
    uint32_t batch_size; // include size of packet header
    uint64_t global_seq_no;
    uint64_t sequencer_timestamp;
} __attribute__((packed));

inline std::ostream& operator<<(std::ostream& os, const PacketHeader& msg) {
    os << "PacketType=PacketHeader"
       << " Size=" << msg.batch_size
       << " GlobalSequenceNumber=" << msg.global_seq_no
       << " SequencerTimestamp=" << TimeNanoToStr(msg.sequencer_timestamp);
    return os;
}

}
