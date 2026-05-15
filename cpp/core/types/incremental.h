#pragma once

#include "bitmap.h"
#include "price.h"
#include "side.h"
#include "message_types.h"
#include "time_types.h"

#include <cstdint>
#include <ostream>

namespace sgt {

// sizeof(Incremental) = 35 with __attribute__ packed
// sizeof(Incremental) = 48 without packed

struct Incremental {
    enum class Flags : uint8_t {};
    uint16_t type = msg_type::INCREMENTAL_L2;
    uint64_t exchange_time;
    Price price;
    Quantity qty = ZERO_QTY;
    uint64_t num_orders_at_level;
    Side side;
    FlagSet<Flags> flag_set;
} __attribute__((packed));

inline std::ostream& operator<<(std::ostream& os, const Incremental& incr) {
    os << "PacketType=IncrementalL2"
       << " ExchangeTime=" << TimeToStr(incr.exchange_time)
       << " Price=" << incr.price.IntValue()
       << " Qty=" << incr.qty.IntValue()
       << " NumOrdersAtLevel=" << incr.num_orders_at_level
       << " Side=" << incr.side;
    return os;
}

}
