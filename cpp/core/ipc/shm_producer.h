#pragma once

#include "macros.h"
#include "spin_lock.h"
#include "ipc/shm_context.h"

#include <atomic>
#include <cstdint>
#include <limits>
#include <thread>
#include <memory>
#include <string.h>
#include <sys/ipc.h>
#include <sys/mman.h>
#include <sys/shm.h>

namespace sgt {

class ShmProducer {
public:
    explicit ShmProducer(ShmContext& context)
    : context_(context)
    {}

    NO_COPY(ShmProducer);

    // For test only
    template<typename T>
    int Produce(const T& obj) {
        return context_.Produce(reinterpret_cast<const char*>(&obj), sizeof(T));
    }

    // For test only
    template<typename T>
    int ProduceWithHeader(const T& obj) {
        return 0;
    }

    int ProduceBatchWithHeader(std::vector<BufferWrapper>& batch, uint16_t topic=0, uint16_t strategy_id = 0) {
        return context_.Produce(batch, topic, strategy_id);
    }

private:
    ShmContext& context_;
};

}
