#pragma once

#include <string>
#include <concepts>

namespace sgt {

enum class Side : uint8_t {
    BID = 0,
    ASK = 1,
    UNKNOWN = 2,
    COUNT
};

inline const char* ToString(Side side) {
    switch (side) {
        case Side::BID: return "BID";
        case Side::ASK: return "ASK";
        default: return "UNKNOWN";
    }
}

inline std::ostream& operator<<(std::ostream& os, const Side side) {
    os << ToString(side);
    return os;
}

template<typename T>
requires std::is_arithmetic_v<T>
Side ToSide(const T& t) {
    if (t > 0) return Side::BID;
    return Side::ASK;
}

enum class PositionSide : uint8_t {
    LONG = 0,
    SHORT,
    BOTH,
    UNKNOWN
};

inline const char* ToString(PositionSide side) {
    switch (side) {
        case PositionSide::LONG: return "LONG";
        case PositionSide::SHORT: return "SHORT";
        case PositionSide::BOTH: return "BOTH";
        default: return "UNKNOWN";
    }
}

}
