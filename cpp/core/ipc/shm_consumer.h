#pragma once

#include "macros.h"
#include "spin_lock.h"

#include <atomic>
#include <cstdint>
#include <limits>
#include <thread>
#include <memory>
#include <iostream>
#include <string.h>
#include <sys/ipc.h>
#include <sys/mman.h>
#include <sys/shm.h>

namespace sgt {

class ShmContext;

class ShmConsumer {
public:
    explicit ShmConsumer(ShmContext& context)
        :context_(context)
    {
        consumer_idx_ = context.GetCursor() + 1;
    }

    ~ShmConsumer() = default;
    NO_COPY(ShmConsumer);

    bool Registered() const { return (consumer_idx_ != -1); }
    int GetConsumerID() const { return consumer_idx_; }

    inline BufferWrapper Consume() {
        auto out = context_.Consume(consumer_idx_);
        consumer_idx_++;
        return out;
    }

    inline BufferWrapper TryConsume() {
        auto out = context_.TryConsume(consumer_idx_);
        if (out.data != nullptr) {
            ++consumer_idx_;
        }
        return out;
    }

    inline BufferWrapper TryConsume(const SubscribedTopics& topic_list) {
        auto out = context_.TryConsume(consumer_idx_, topic_list);
        consumer_idx_ = out.second;
        return out.first;
    }

    inline std::pair<int64_t, int64_t> Getcursor() const {
        return {consumer_idx_, context_.GetCursor()};
    }

    void Release() { /* noop */ }

private:
    ShmContext& context_;
    int64_t consumer_idx_ = -1;
    bool deferred_init_ = false;
};

}
