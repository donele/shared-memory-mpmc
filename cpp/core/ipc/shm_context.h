#pragma once

#include "spin_lock.h"
#include "message_types.h"
#include "packet_header.h"

#include <set>
#include <bitset>
#include <atomic>
#include <cstdint>
#include <limits>
#include <thread>
#include <cstring>
#include <chrono>
#include <iostream>
#include <string.h>
#include <sys/ipc.h>
#include <sys/mman.h>
#include <sys/shm.h>

// ftok
//  Converts a pathname to IPC key
// shmget
//   Allocates a shared memory segment
// shmat(shmid, shmaddr, shmflg)
//   Attaches shared memory segment of shmid to teh address space of calling proc
//
// Header                                   <--- header_, queue_ptr_
// ConsumerSequence * consumer_sequence_num <--- header_ + 1, reader_sequence_numbers_
// Metadata (ledger_) * num_consumers       <--- reader_sequence_numbers_ + num_consumers, ledger_
// storage_ (char*)                         <--- ledger_ + actual_ledger_size, storage_
//
// mmap
//   Map a file (, shm, or typed memory object) to a process' address space

namespace sgt {

struct SubscribedTopics {
    std::bitset<64> topics;
    std::set<uint16_t> strategy_ids;

    void set() { topics.set(); }
    void set(size_t bit, bool value = true) { topics.set(bit, value); }
    void reset() { topics.reset(); }
    void reset(size_t bit) { topics.reset(bit); }
    void set_strategy_ids(const std::set<uint16_t>& strat_ids) { strategy_ids = strat_ids; }
    void set_strategy_ids(uint16_t strat_id) { strategy_ids.insert(strat_id); }
    bool IsIncluded(int strat_id) const {
        return (0 == strat_id || (!strategy_ids.empty() && strategy_ids.count(0) != 0)
                || strategy_ids.count(strat_id) != 0);
    }
};

struct ConsumerStats {
    int64_t sequence_num;
    pid_t consumer_pid;
    bool occupied;
};

class ShmContext {
    struct Header {
        alignas(64) std::atomic_int64_t sequence_num;
        alignas(64) std::atomic_int64_t claimed_sequence_num;
        alignas(64) std::atomic_int64_t current_offset_;
        alignas(64) AtomicFlagSpinlock reader_index_lock;
        int64_t LEDGER_SIZE;
        int64_t STORAGE_SIZE;
        int64_t CONSUMERS;
        std::atomic_bool initialized_ = false;
    };

    struct alignas(64) ConsumerSequence {
        std::atomic_int64_t sequence_num = 0;
        std::atomic_int64_t offset = 0;
        pid_t pid = 0;
        std::atomic_bool done = false;
        std::atomic_bool occupied = false;
    };

    struct Metadata {
        int64_t offset;
        int64_t size;
        uint16_t topic;
        uint16_t strategy_id;
    };

public:
    explicit ShmContext(std::string name)
        :name_(name),
        memory_size_(0),
        header_(nullptr),
        reader_sequence_numbers_(nullptr),
        ledger_(nullptr),
        storage_(nullptr)
    {}

    ~ShmContext() {
        if(queue_ptr_ == nullptr) return;
        shmdt(queue_ptr_);
    }

    int Create(int64_t queue_size, int64_t num_consumers, int64_t obj_size_hint) {
        auto actual_queue_size = GetNextPowerOfTwo(queue_size);
        int64_t actual_ledger_size = actual_queue_size / GetNextPowerOfTwo(obj_size_hint);
        int64_t total_size = sizeof(Header) + (sizeof(ConsumerSequence) * num_consumers)
            + (actual_ledger_size * sizeof(Metadata)) + actual_queue_size;
        key_t shm_key = ftok(name_.c_str(), 8);
        auto shm_id = shmget(shm_key, total_size, IPC_CREAT | 0666);
        if(shm_id < 0)  {
            perror("shmget");
            return -1;
        }

        queue_ptr_ = shmat(shm_id, NULL, 0);
        memset(queue_ptr_, 0, total_size);

        header_ = reinterpret_cast<Header*>(queue_ptr_);
        header_->LEDGER_SIZE = actual_ledger_size;
        header_->STORAGE_SIZE = actual_queue_size;
        header_->CONSUMERS = num_consumers;

        reader_sequence_numbers_ = reinterpret_cast<ConsumerSequence*>(header_ + 1);
        ledger_ = reinterpret_cast<Metadata*>(reader_sequence_numbers_ + num_consumers);
        storage_ = reinterpret_cast<char*>(ledger_ + actual_ledger_size);

        header_->initialized_.store(true);
        return 0;
    }

    int Attach() {
        key_t shm_key = ftok(name_.c_str(), 8);
        auto shm_id = shmget(shm_key, 0, 0);
        if(shm_id < 0) {
            perror("shmget");
            return -1;
        }
        queue_ptr_ = shmat(shm_id, NULL, 0);
        if(queue_ptr_ == MAP_FAILED) {
            perror("shmat");
            return -1;
        }
        header_ = reinterpret_cast<Header*>(queue_ptr_);
        while(!header_->initialized_)
            ;
        reader_sequence_numbers_ = reinterpret_cast<ConsumerSequence*>(header_ + 1);
        ledger_ = reinterpret_cast<Metadata*>(reader_sequence_numbers_ + header_->CONSUMERS);
        storage_ = reinterpret_cast<char*>(ledger_ + header_->LEDGER_SIZE);
        return 0;
    }

    int Destroy() {
        key_t shm_key = ftok(name_.c_str(), 8);
        auto shmid = shmget(shm_key, 0, 0);
        if(shmid < 0) {
            perror("shmget");
        }
        return shmctl(shmid, IPC_RMID, NULL);
    }

    int64_t GetLedgerSize() const { return header_->LEDGER_SIZE; }
    int64_t GetStorageSize() const { return header_->STORAGE_SIZE; }
    int64_t GetConsumers() const { return header_->CONSUMERS; }

    int64_t GetCursor() const {
        return header_->sequence_num.load(std::memory_order_relaxed);
    }

    const char* const GetStorage() const { return storage_; }

    // Write single data packet. No seq num, no timestamp.
    int Produce(const char* data, int64_t size, uint16_t topic = 0, uint16_t strategy_id = 0) {
        // Get the sequence_num for the intended write.
        auto claimed_expected = header_->claimed_sequence_num.load(std::memory_order_acquire); // A
        auto claimed_desired = claimed_expected + 1;
        const auto LEDGER_SIZE = header_->LEDGER_SIZE;
        const auto STORAGE_SIZE = header_->STORAGE_SIZE;
        while(!header_->claimed_sequence_num.compare_exchange_weak(
                    claimed_expected, claimed_desired, std::memory_order_acq_rel)) { // /A
            claimed_desired = claimed_expected + 1;
        }

        // Wait for the other thread to be done writing.
        int64_t expected = claimed_desired - 1;
        int64_t count = 0;
        while(header_->sequence_num.load(std::memory_order_acquire) != expected) { // B
            ++count;
            if(count >= NUM_ITER) [[unlikely]] {
                std::this_thread::yield();
                count = 0;
            }
        }

        // Check remaining sizes
        auto curr_offset = header_->current_offset_.load(std::memory_order_acquire); // C
        auto remaining_size = STORAGE_SIZE - (curr_offset & STORAGE_SIZE - 1) - 1;
        int64_t offset_start = 0;
        int64_t offset_end = 0;
        if(size > remaining_size) [[unlikely]] {
            offset_start = curr_offset + remaining_size + 1;
            offset_end = offset_start + size - 1;
        } else {
            offset_start = curr_offset + 1;
            offset_end = offset_start + size - 1;
        }

        // First index is 1.
        // If the number exceeds LEDGER_SIZE, recycle ledger_.
        ledger_[claimed_desired & (LEDGER_SIZE - 1)].offset = offset_start;
        ledger_[claimed_desired & (LEDGER_SIZE - 1)].size = size;
        ledger_[claimed_desired & (LEDGER_SIZE - 1)].topic = topic;
        ledger_[claimed_desired & (LEDGER_SIZE - 1)].strategy_id = strategy_id;

        // Write the data
        std::memcpy(&storage_[offset_start & STORAGE_SIZE - 1], data, size);

        header_->current_offset_.store(offset_end, std::memory_order_relaxed); // /B
        header_->sequence_num.store(claimed_desired, std::memory_order_release); // /C
        return 0;
    }

    // Write PachetHeader, batch data, seq num, and timestamp.
    int Produce(std::vector<BufferWrapper>& batch, uint16_t topic = 0, uint16_t strategy_id = 0) {
        // Get the sequence_num for the intended write.
        auto claimed_expected = header_->claimed_sequence_num.load(std::memory_order_acquire); // A
        auto claimed_desired = claimed_expected + 1;
        const auto LEDGER_SIZE = header_->LEDGER_SIZE;
        const auto STORAGE_SIZE = header_->STORAGE_SIZE;
        while(!header_->claimed_sequence_num.compare_exchange_weak(
                    claimed_expected, claimed_desired, std::memory_order_acq_rel)) { // /A
            claimed_desired = claimed_expected + 1;
        }

        // Wait for the other thread to be done writing.
        int64_t expected = claimed_desired - 1;
        int64_t count = 0;
        while(header_->sequence_num.load(std::memory_order_acquire) != expected) { // B
            ++count;
            if(count >= NUM_ITER) [[unlikely]] {
                std::this_thread::yield();
                count = 0;
            }
        }

        // Check remaining sizes
        int64_t data_size = 0;
        for (const auto& packet : batch) { data_size += packet.size; }
        uint32_t size_to_write = sizeof(ph_) + data_size;
        auto curr_offset = header_->current_offset_.load(std::memory_order_acquire); // C
        auto remaining_size = STORAGE_SIZE - (curr_offset & STORAGE_SIZE - 1) - 1;
        int64_t offset_start = 0;
        int64_t offset_end = 0;
        if(size_to_write > remaining_size) [[unlikely]] {
            offset_start = curr_offset + remaining_size + 1;
            offset_end = offset_start + size_to_write - 1;
        } else {
            offset_start = curr_offset + 1;
            offset_end = offset_start + size_to_write - 1;
        }

        // First index is 1.
        // If the number exceeds LEDGER_SIZE, recycle ledger_.
        ledger_[claimed_desired & (LEDGER_SIZE - 1)].offset = offset_start;
        ledger_[claimed_desired & (LEDGER_SIZE - 1)].size = size_to_write;
        ledger_[claimed_desired & (LEDGER_SIZE - 1)].topic = topic;
        ledger_[claimed_desired & (LEDGER_SIZE - 1)].strategy_id = strategy_id;

        // Write the packet header
        ph_.batch_size = size_to_write;
        ph_.global_seq_no = claimed_desired;
        ph_.sequencer_timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        auto curr_idx = offset_start & STORAGE_SIZE - 1;
        std::memcpy(storage_ + curr_idx, &ph_, sizeof(ph_));
        curr_idx += sizeof(ph_);

        // Write the data
        for (const auto& packet : batch) {
            std::memcpy(storage_ + curr_idx, packet.data, packet.size);
            curr_idx += packet.size;
        }

        header_->current_offset_.store(offset_end, std::memory_order_relaxed); // /B
        header_->sequence_num.store(claimed_desired, std::memory_order_release); // /C
        return 0;
    }

    BufferWrapper Consume(int64_t consumer_sequence) {
        while(header_->sequence_num.load(std::memory_order_acquire) < consumer_sequence) {}
        int64_t idx = consumer_sequence & (header_->LEDGER_SIZE - 1); // ledger index
        auto offset = ledger_[idx].offset;
        auto size = ledger_[idx].size;
        auto topic = ledger_[idx].topic;
        auto strategy_id = ledger_[idx].strategy_id;
        auto storage_idx = offset & (header_->STORAGE_SIZE - 1);
        return BufferWrapper{storage_ + storage_idx, size, topic, strategy_id};
    }

    // returns size = -1 if cannot be consumed.
    BufferWrapper TryConsume(int64_t consumer_sequence) {
        if (header_->sequence_num.load(std::memory_order_acquire) < consumer_sequence) {
            return BufferWrapper{ nullptr, -1, 0, 0};
        }
        int64_t idx = consumer_sequence & (header_->LEDGER_SIZE - 1);
        auto offset = ledger_[idx].offset;
        auto size = ledger_[idx].size;
        auto topic = ledger_[idx].topic;
        auto strategy_id = ledger_[idx].strategy_id;
        auto storage_idx = offset & (header_->STORAGE_SIZE - 1);
        return BufferWrapper{ storage_ + storage_idx, size, topic, strategy_id };
    }

    // Try consume with topic
    std::pair<BufferWrapper, int64_t> TryConsume(int64_t consumer_sequence, const SubscribedTopics& topics) {
        auto global_seq_no = header_->sequence_num.load(std::memory_order_acquire);
        if (global_seq_no < consumer_sequence) { return std::make_pair(BufferWrapper{ nullptr, -1, 0, 0},
                consumer_sequence); }
        while (consumer_sequence <= global_seq_no) {
            int64_t idx = consumer_sequence & (header_->LEDGER_SIZE - 1);
            auto topic = ledger_[idx].topic;
            auto strategy_id = ledger_[idx].strategy_id;

            if (topics.topics.test(topic) && topics.IsIncluded(strategy_id)) {
                auto offset = ledger_[idx].offset;
                auto size = ledger_[idx].size;
                return std::make_pair(BufferWrapper{ storage_ + (offset & header_->STORAGE_SIZE - 1),
                        size, topic, strategy_id}, consumer_sequence + 1);
            }
            ++consumer_sequence;
        }
        return std::make_pair(BufferWrapper{ nullptr, -1, 0, 0 }, consumer_sequence);
    }

    int64_t GetMinConsumerSequence() {
        int64_t output = std::numeric_limits<int64_t>::max();
        for (int i = 0; i < header_->CONSUMERS; ++i) {
            if (reader_sequence_numbers_[i].occupied) {
                output = std::min(output, reader_sequence_numbers_[i].sequence_num.load(std::memory_order_relaxed));
            }
        }
        return output;
    }

    void GetConsumerStats(std::vector<ConsumerStats>& output) {
        if (output.size() != header_->CONSUMERS) { output.resize(header_->CONSUMERS); }
        for (int i = 0; i < header_->CONSUMERS; ++i) {
            output[i].occupied = reader_sequence_numbers_[i].occupied.load(std::memory_order_relaxed);
            output[i].sequence_num = reader_sequence_numbers_[i].sequence_num.load(std::memory_order_relaxed);
            output[i].consumer_pid = reader_sequence_numbers_[i].pid;
        }
    }

    int64_t GetCurrentSequenceNumber() { return header_->sequence_num.load(std::memory_order_relaxed); }
    int64_t GetClaimedSequenceNumber() { return header_->claimed_sequence_num.load(std::memory_order_relaxed); }
    static int64_t GetNextPowerOfTwo(int64_t size) {
        int64_t i{ 0 };
        if (size > std::numeric_limits<int64_t>::max() / 2) { return -1; }
        for (; (1L << i) < size; ++i)
            ;
        return 1L << i;
    }

private:
    std::string name_;
    uint64_t memory_size_;
    void* queue_ptr_ = nullptr;
    Header* header_;
    PacketHeader ph_;
    ConsumerSequence* reader_sequence_numbers_;
    Metadata* ledger_;
    char* storage_;

    static constexpr int64_t NUM_ITER = 1024;
};

}
