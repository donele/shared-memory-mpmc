#pragma once

#include <cstdint>

namespace sgt {

struct Bitmap32 {
    uint32_t bits{ 0 };
    void Set(uint64_t b) {
        if (b > 31) return;
        bits |= 1 << b;
    }
    void Flip(uint64_t b) {
        if (b > 31) return;
        bits ^= 1 << b;
    }
    void Reset(uint64_t b) {
        if (b > 31) return;
        bits &= ~(static_cast<unsigned int>(1) << b);
    }
    bool Get(uint64_t b) const {
        if (b > 31) return false;
        auto tmp = bits;
        return (tmp >> b) & 1;
    }
    void ClearMap() { bits = 0; }
}__attribute__((packed));

template<typename F>
struct FlagSet {
    Bitmap32 bitmap;
    void Set(F flag) { bitmap.Set(static_cast<uint64_t>(flag)); }
    void Flip(F flag) { bitmap.Flip(static_cast<uint64_t>(flag)); }
    void Reset(F flag) { bitmap.Reset(static_cast<uint64_t>(flag)); }
    bool Get(F flag) const { return bitmap.Get(static_cast<uint64_t>(flag)); }
    void Clear() { bitmap.ClearMap(); }
}__attribute__((packed));

}
