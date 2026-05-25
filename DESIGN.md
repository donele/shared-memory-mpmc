# Design

This repository contains four implementations of the same basic shared-memory publishing idea:

- `cpp/`: the minimal C++ subset copied from `sgt`
- `java-jdk/`: a plain JDK mmap implementation
- `java-agrona/`: an Agrona-based off-heap queue
- `java-mpmc/`: a custom Java off-heap queue designed to follow the C++ structure more closely

This document explains:

- how each version is laid out
- how publication and consumption work
- where each version is fast
- where each version pays overhead
- why the benchmark numbers ended up much closer than the cold-run numbers suggested

## Shared workload

All versions publish the same logical test message:

- message type
- exchange timestamp
- price
- quantity
- number of orders at level
- side

The benchmark loops publish 10,000 messages per timed run.

The producer benchmarks mainly measure:

- reservation overhead
- metadata updates
- payload copy / payload store cost
- publication ordering cost

They do not include:

- network input
- parsing
- business logic
- persistence
- complex batching behavior

## C++

### Relevant files

- `cpp/core/ipc/shm_context.h`
- `cpp/core/ipc/shm_producer.h`
- `cpp/core/ipc/shm_consumer.h`
- `cpp/bin/shm_manager.cpp`
- `cpp/bin/shm_test_producer.cpp`
- `cpp/bin/shm_test_consumer_batch.cpp`

### Layout

The C++ queue uses one shared-memory region split into:

1. header
2. consumer state array
3. ledger array
4. payload storage ring

The header contains:

- `sequence_num`
- `claimed_sequence_num`
- `current_offset_`
- queue sizes
- initialization flag

The ledger contains one entry per published message:

- payload offset
- payload size
- topic
- strategy id

The payload storage ring contains the raw bytes written by producers.

### Producer algorithm

The main hot path is `ShmContext::Produce(...)`.

For each message it does:

1. acquire-load `claimed_sequence_num`
2. `compare_exchange_weak` loop to reserve the next sequence
3. spin until `sequence_num == reserved_sequence - 1`
4. acquire-load `current_offset_`
5. compute contiguous or wrapped storage range
6. write the ledger entry
7. `memcpy` the payload into the ring
8. store the new `current_offset_`
9. release-store the new `sequence_num`

This is a custom serialized publication protocol:

- multiple producers can race to claim the next sequence
- only one producer can make the next sequence visible
- consumers use `sequence_num` as the publish point

### Consumer algorithm

The current consumer path is not a competing-consumer work queue.

Each consumer starts from the current cursor and independently follows the published stream.
This design behaves more like pub-sub over shared memory than a work queue that hands each
message to exactly one consumer.

### Performance characteristics

- direct shared-memory layout
- direct payload `memcpy`
- no generic record abstraction
- one explicit publish point
- minimal metadata

Costs on the hot path:

- multi-producer safety requires a CAS
- publication is globally serialized by sequence
- wrap handling is on the hot path
- both metadata and payload are written per message

### Important detail: actual payload size

The copied `cpp/core/types/incremental.h` still contains a stale comment claiming:

- `sizeof(Incremental) = 35` when packed

That comment no longer matches the copied type definitions. With the current `Price`,
`Quantity`, `Side`, and `FlagSet` definitions, the struct is `55` bytes:

- `type`: 2
- `exchange_time`: 8
- `price`: 16
- `qty`: 16
- `num_orders_at_level`: 8
- `side`: 1
- `flag_set`: 4

This matters when comparing write bandwidth against the Java versions, which currently encode
a `35` byte wire-format message instead of writing the larger C++ struct directly.

## Java JDK

### Relevant files

- `java-jdk/src/SharedMemory.java`
- `java-jdk/src/ShmManager.java`
- `java-jdk/src/ShmTestProducer.java`
- `java-jdk/src/ShmTestConsumerBatch.java`

### Layout

The plain JDK version uses a file-backed `MappedByteBuffer` with a manually defined layout:

1. header
2. consumer slots
3. ledger entries
4. storage ring

The layout intentionally follows the C++ version.

### Producer algorithm

The producer reserves a region, writes ledger metadata, writes the encoded incremental payload
into the mapped storage ring, and advances the published sequence.

The optimized path avoids per-message allocation by calling `produceIncremental(...)` and writing
the fields directly into off-heap storage.

### Consumer algorithm

The consumer:

1. reads the current cursor
2. spins until the target sequence is published
3. reads the ledger
4. copies the payload bytes out
5. decodes the message

### Performance characteristics

- direct mapped-buffer access
- small encoded payload (`35` bytes)
- no generic queue framework
- no per-message allocation in the optimized producer path

Limits and tradeoffs:

- it does not use the strongest low-level atomic protocol
- `MappedByteBuffer` access is still higher-level than raw native pointers
- correctness relies on a simpler publication model than the C++ path

### Semantic caveat

This is the simplest Java baseline in the repo. It isolates:

- the cost of file-backed off-heap access
- the cost of the manual wire format
- the cost of a very small producer hot path

## Java Agrona

### Relevant files

- `java-agrona/src/main/java/AgronaShm.java`
- `java-agrona/src/main/java/ShmManager.java`
- `java-agrona/src/main/java/ShmTestProducer.java`
- `java-agrona/src/main/java/ShmTestConsumerBatch.java`

### Layout

This version uses:

- `MappedByteBuffer`
- `UnsafeBuffer`
- `OneToOneRingBuffer`
- `ExpandableArrayBuffer`

The data is off-heap and file-backed, and the queue protocol uses Agrona's record format rather
than the custom ledger-plus-storage format used in the C++ code.

### Producer algorithm

The producer first encodes the message into a reusable Agrona buffer.
Then `OneToOneRingBuffer.write(...)`:

- checks type and message length
- claims capacity
- handles alignment and padding
- writes a temporary negative length
- issues a release fence
- copies payload bytes into the ring
- writes the type
- ordered-writes the final record length

### Consumer algorithm

The consumer uses `ringBuffer.read(handler, limit)`.
Agrona scans from the current head, interprets record headers, skips padding records, and invokes
the message handler for real data.

### Performance characteristics

- optimized off-heap buffer implementation
- explicit ordered / volatile memory access inside Agrona
- reusable encoding buffer
- highly tuned library code

Costs and limits:

- generic record abstraction adds header / alignment overhead
- the producer encodes into a buffer and Agrona copies that buffer into the ring
- the current queue type is `OneToOneRingBuffer`, so it is not a direct semantic clone of the C++
  multi-producer publication path

### Important semantic difference

This version is SPSC in the queue primitive.

This makes it a high-performance off-heap Java queue, but it does not match the many-producer /
many-consumer semantics of the C++ design.

## Java MPMC

### Relevant files

- `java-mpmc/src/MpmcSharedMemory.java`
- `java-mpmc/src/ShmManager.java`
- `java-mpmc/src/ShmTestProducer.java`
- `java-mpmc/src/ShmTestConsumerBatch.java`

### Design goal

This version mirrors the C++ queue structure and protocol more closely than the other Java
variants.

It is a custom off-heap queue built from:

- file-backed `MappedByteBuffer`
- `VarHandle` atomic access over the mapped bytes
- a header / consumer-slot / ledger / storage layout

### Layout

The file layout is:

1. header
2. fixed consumer slots
3. ledger entries
4. storage ring

The header contains:

- magic
- version
- initialization flag
- ledger size
- storage size
- published sequence
- claimed sequence
- current offset
- consumer count

Consumer slots contain:

- next-sequence snapshot
- active flag

Ledger entries contain:

- offset
- size
- topic
- strategy id

### Producer algorithm

The producer path intentionally copies the C++ idea:

1. CAS the claimed sequence
2. spin until the previous sequence is published
3. read the current payload offset
4. compute wrap or contiguous write
5. write the ledger entry
6. write the payload directly into storage
7. update the current offset
8. release-store the published sequence

This version uses `VarHandle` directly for off-heap CAS and ordered publication.

### Consumer algorithm

Consumers register for a fixed slot and then:

1. track their own `nextSequence`
2. spin until that sequence is published
3. read the ledger
4. copy the payload bytes
5. update their slot with the next sequence

Like the C++ code, this is still a pub-sub stream:

- every consumer can independently read the stream
- consumers do not divide the stream among themselves

### Performance characteristics

- direct write into final storage
- no generic record abstraction
- encoded payload is only `35` bytes
- explicit CAS and release publication on off-heap bytes

Costs on the hot path:

- it still pays a producer CAS, just like C++
- publication is still serialized by the published sequence
- `MappedByteBuffer` plus `VarHandle` is still not as direct as C++ pointers
- startup is intentionally slow because the current manager zero-fills the whole `1 GiB` file

## Why the measured numbers converged

The first Java measurements looked much worse because the benchmark included cold-JVM effects.
After repeated runs, the producer times converged into the same broad range.

The steady-state numbers converged for a few reasons:

- the workload is extremely small
- all versions publish a small message into warm shared memory
- allocation was removed from the Java hot paths
- steady-state JIT-compiled code is much faster than cold Java startup

The main takeaway:

- cold run latency and steady-state latency are different measurements

## Benchmark interpretation

The producer-only benchmark is useful, but narrow.

It measures:

- per-message queue overhead
- payload publication overhead

It does not capture:

- contention between multiple producers
- behavior with several attached consumers
- backpressure under occupancy
- large payload effects
- end-to-end application work

## Comparison summary

### C++

- closest to the original source system
- strong custom publication protocol
- low abstraction cost
- direct `memcpy` payload path

### Java JDK

- simplest Java baseline
- direct mmap path
- weaker concurrency model than the custom C++ path
- surprisingly competitive in the producer-only microbenchmark

### Java Agrona

- best library-based off-heap queue here
- tuned ordered/volatile internals
- extra generic record overhead
- SPSC semantics in the chosen primitive

### Java MPMC

- closest semantic Java match to the C++ design
- custom off-heap queue
- explicit claimed-sequence / published-sequence protocol
- best apples-to-apples Java comparison point for the C++ implementation
