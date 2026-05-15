#pragma once

#include <atomic>
#include <memory>
#include <vector>

namespace sgt {

class AtomicFlagSpinlock {
    enum { MAX_SPINS = 5 };

    std::atomic_flag barrier_ = ATOMIC_FLAG_INIT;
    const size_t max_spins_ = MAX_SPINS;
    struct timespec sleep_time_;

public:
    AtomicFlagSpinlock() {
        sleep_time_.tv_sec = 0;
        sleep_time_.tv_nsec = 1;
    }

    AtomicFlagSpinlock(size_t nsec, size_t maxSpins)
        :max_spins_(maxSpins)
    {
        sleep_time_.tv_sec = 0;
        sleep_time_.tv_nsec = 1;
    }

    ~AtomicFlagSpinlock() {
        unlock();
    }

    void lock() {
        size_t cnt = max_spins_;
        while (barrier_.test_and_set(std::memory_order_acquire)) {
            --cnt;
            if (0 == cnt) {
                nanosleep(&sleep_time_, 0);
                cnt = max_spins_;
            }
        }
    }

    void unlock() {
        barrier_.clear(std::memory_order_release);
    }

};

}
