#pragma once

#include <cmath>
#include <cassert>

namespace sgt {

#define NO_COPY(T) T(const T&) = delete; T& operator=(const T&) = delete
#define NO_MOVE(T) T(T&&) = delete; T& operator=(T&&) = delete

template<typename T>
void check_value(T x) {
    if constexpr (std::is_floating_point_v<T>) {
        assert(!std::isnan(x) && std::abs(x) < 1e12);
    } else {
        assert(std::abs(x) < 1e12);
    }
}

}
