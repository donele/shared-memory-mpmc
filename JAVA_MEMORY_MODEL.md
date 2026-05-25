# Java Memory Model

This repository has three distinct Java shared-memory implementations:

- `java-jdk/`: plain JDK `MappedByteBuffer`
- `java-agrona/`: `MappedByteBuffer` wrapped by Agrona `UnsafeBuffer`
- `java-mpmc/`: `MappedByteBuffer` plus `VarHandle` byte-buffer views

All three versions also use short-lived or reusable heap buffers in a few encode/decode helper
paths.

This file describes each buffer type, how the repo uses it, and what that means for
performance and semantics.

## Buffer Families

### `MappedByteBuffer`

Used by:

- `java-jdk/src/SharedMemory.java`
- `java-agrona/src/main/java/AgronaShm.java`
- `java-mpmc/src/MpmcSharedMemory.java`

What it is:

- a file-backed memory mapping exposed through the JDK `ByteBuffer` API
- off-heap from the JVM perspective
- shared through the operating system page cache across processes that map the same file

Why it matters here:

- it gives all Java variants access to shared memory without JNI
- reads and writes land in a mapped region instead of ordinary heap objects
- the backing storage is persistent enough to be inspected by multiple processes, but access still
  goes through Java buffer methods rather than raw native pointers

Performance implications:

- better than copying through ordinary heap files or sockets for this use case
- still higher-level than C++ pointer arithmetic and direct atomic instructions
- method dispatch, bounds checks, and JDK memory-access rules remain part of the hot path
- random single-field access is usually fine; very tight per-field loops can still cost more than a
  native implementation

Operational implications:

- startup can be slow when a large file is zero-filled eagerly
- `force()` pushes dirty pages toward the backing file and is not something to do on the hot path
- lifetime is tied to both the file descriptor and the mapping; Java does not make explicit unmap
  especially ergonomic

### `ByteBuffer`

Used by:

- direct inheritance from `MappedByteBuffer`
- `duplicate()` views in `java-jdk/src/SharedMemory.java`
- `duplicate()` views in `java-mpmc/src/MpmcSharedMemory.java`
- `ByteBuffer.allocate(...)` and `ByteBuffer.wrap(...)` helper paths in the JDK and MPMC versions

What it is:

- the general JDK buffer abstraction
- can be heap-backed or direct/off-heap depending on how it is created

In this repo, `ByteBuffer` plays two distinct roles:

1. mapped/off-heap access through `MappedByteBuffer`
2. temporary heap buffers for encode/decode convenience

Performance implications:

- `duplicate()` is useful because it creates another view without copying bytes
- that is cheaper than allocating a new array, but it is still another object and another layer of
  API indirection
- `ByteBuffer.allocate(...)` is heap allocation, so it is fine for helper utilities and bad for a
  producer hot path if done per message

The current code already reflects that tradeoff:

- the optimized producer paths write fields directly into mapped storage
- helper methods like `encodeIncremental(...)` and `decodeIncremental(...)` still use heap
  `ByteBuffer` objects because clarity matters more there than shaving every allocation

### Agrona `UnsafeBuffer`

Used by:

- `java-agrona/src/main/java/AgronaShm.java`

What it is:

- Agrona’s off-heap/direct-buffer implementation
- a thin wrapper around memory that exposes fast primitive access plus ordered / volatile accessors
  used by Agrona queue implementations

How it is used here:

- the underlying storage is still a `MappedByteBuffer`
- `UnsafeBuffer` wraps that mapping
- `OneToOneRingBuffer` then uses the `UnsafeBuffer` as its storage and metadata area

Why it often runs faster than plain JDK buffer access:

- Agrona is built specifically for low-latency messaging and off-heap data structures
- primitive accessors and queue code are heavily optimized
- the library already encodes memory-ordering rules into its API instead of leaving them implicit

Why it is not automatically the fastest path in this repo:

- it still sits on top of the same mapped file substrate
- the ring buffer uses Agrona’s generic record format with headers, alignment, and padding
- the producer writes into a reusable message buffer and then Agrona copies that payload into the
  ring, so there is an extra copy that the direct-write JDK and MPMC paths avoid

### Agrona `ExpandableArrayBuffer`

Used by:

- `java-agrona/src/main/java/AgronaShm.java`
- `java-agrona/src/main/java/ShmTestProducer.java`

What it is:

- a heap-backed, growable Agrona message buffer
- implements `MutableDirectBuffer`

How it is used here:

- as a reusable staging buffer for encoding the incremental message before handing it to the ring

Performance implications:

- much better than allocating a fresh heap object per message
- still not zero-copy end to end, because the encoded message is copied from this staging buffer
  into the ring buffer
- the growable design is flexible, but flexibility is not free compared with writing directly into
  the final destination

The distinction matters:

- reusable staging buffer removes allocation pressure
- it does not remove the payload copy into the queue

### Agrona `MutableDirectBuffer` and `DirectBuffer`

Used by:

- `MutableDirectBuffer` for producer-side encoding
- `DirectBuffer` for consumer-side decoding in `AgronaShm.readIncremental(...)`

What they are:

- Agrona interfaces for writable and readable primitive access
- they abstract over the actual storage implementation

Why they are useful:

- the producer code can encode fields without caring whether the underlying storage is heap or
  direct
- the consumer code can decode without first translating into an intermediate Java object layout

Performance implications:

- interface-based code remains fast because the concrete implementations are tuned for this use case
- but abstraction does not erase the ring-buffer protocol costs around it

### `VarHandle` byte-buffer views

Used by:

- `java-mpmc/src/MpmcSharedMemory.java`

What they are:

- JDK atomics over byte-buffer-backed memory using
  `MethodHandles.byteBufferViewVarHandle(...)`
- this repo creates handles for `long[]` and `int[]` views over the mapped buffer

How they are used here:

- CAS on the claimed sequence
- acquire reads for initialization and size visibility
- release stores for publication and consumer progress
- volatile reads for the shared producer/consumer cursors

Why this matters:

- this is the strongest concurrency story in the Java codebase
- unlike the plain JDK implementation, it makes the publication protocol explicit
- the code more closely mirrors the C++ logic: claim, write, publish

Performance implications:

- CAS and acquire/release semantics cost more than plain non-atomic stores
- that cost is necessary for correctness in the multi-producer publication path
- even so, it can still perform well because the payload is small and the data is written directly
  into the final mapped storage

Important tradeoff:

- stronger semantics usually improve correctness first, not raw microbenchmark speed
- for a trivial single-producer test, the simpler JDK path may appear competitive because it avoids
  some atomic coordination that true MPMC semantics require

## `java-mpmc/src/MpmcSharedMemory.java`

This class is the core of the custom Java MPMC variant. It owns the mapped file, defines the
shared-memory layout, performs producer reservation and publication, and tracks consumer progress.

### What the class stores

The class keeps:

- the mapped file handle and `MappedByteBuffer`
- the queue geometry: header, consumer-slot area, ledger area, and storage ring
- static offsets for every shared field in the header and ledger
- reusable `VarHandle` accessors for atomic reads and writes on mapped bytes

The layout matches the queue description in `ARCHITECTURE.md`: a header followed by consumer
slots, ledger entries, and the payload storage ring.

### The `VarHandle` definitions

The class defines:

- `private static final VarHandle LONG_HANDLE = MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);`
- `private static final VarHandle INT_HANDLE = MethodHandles.byteBufferViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);`

Each part matters:

- `MethodHandles` is the JDK factory class in `java.lang.invoke` that creates low-level runtime
  handles
- `VarHandle` is the handle object that knows how to read and write a specific kind of memory
  location
- `byteBufferViewVarHandle(...)` creates a handle that treats a `ByteBuffer` as a typed sequence
  of primitive values
- `long[].class` and `int[].class` specify the element type of that view
- `ByteOrder.BIG_ENDIAN` tells the handle how to interpret the bytes in the mapped buffer

The `[]` in `long[].class` is part of the API contract. This factory expects an array class
because it models the `ByteBuffer` as an indexed view over repeated primitive elements. The code
is not creating a Java `long[]` object here. It is using the array class token to say "treat this
buffer as a sequence of `long` values."

The handle names are capitalized because they are `private static final` constants. The code
creates each handle once and reuses it for the lifetime of the class.

### What a handle means here

A handle is an indirect typed accessor. `LONG_HANDLE` does not store a `long` value and it does
not point at one fixed location. It stores the logic for accessing `long` fields in a
`ByteBuffer` when the caller supplies:

- the target buffer
- the byte offset within that buffer
- the access mode, such as volatile read, release store, or compare-and-set

That is why helper methods such as `getLongVolatile(buffer, offset)` and
`setLongRelease(buffer, offset, value)` are small wrappers around the same handle.

### How the access modes map to the protocol

The class uses a small set of access patterns:

- `LONG_HANDLE.compareAndSet(...)` reserves the next claimed producer sequence
- `LONG_HANDLE.getVolatile(...)` reads shared sequence and offset fields that producers and
  consumers observe concurrently
- `INT_HANDLE.getAcquire(...)` reads initialization, version, and ledger-size style fields after
  the writer has published them
- `LONG_HANDLE.setRelease(...)` publishes a sequence only after the payload and ledger entry are in
  place
- `INT_HANDLE.setRelease(...)` publishes initialization and consumer active-state changes

This is the direct Java equivalent of the queue protocol the class is trying to mirror:

1. claim a sequence with CAS
2. wait until the prior sequence is published
3. write ledger metadata
4. write payload bytes
5. release-publish the new visible sequence

The plain `buffer.putLong(...)`, `putInt(...)`, and `putShort(...)` helpers are used for ordinary
writes that happen before the release store. The release operation is the publish point that makes
those earlier writes visible in the intended order.

### Why `MethodHandles` appears at all

`MethodHandles` is the JDK entry point for creating these low-level access handles. The name is a
little broader than the specific class use here. It does not mean this code is dealing with Java
methods directly. In this class, `MethodHandles` is only acting as the factory that creates the
`VarHandle` instances for byte-buffer-backed integer and long fields.

### Storage and wrap behavior

The class uses the handles for control metadata in the header and consumer slots. Payload bytes are
written separately:

- `writeShortToStorage(...)` and `writeLongToStorage(...)` write directly into the mapped storage
  ring
- if the value fits contiguously before the end of the ring, the code uses `buffer.putShort(...)`
  or `buffer.putLong(...)`
- if the value crosses the ring boundary, the code falls back to byte-by-byte writes in big-endian
  order

The read side uses `buffer.duplicate()` to copy bytes out of the storage ring, handling the
wrapped case in two chunks when necessary.

### Why this class matters in the repo

`MpmcSharedMemory.java` is where the custom Java implementation stops being a generic mapped-file
example and becomes a real queue protocol. It is the clearest place in the repo to study:

- how the header fields are laid out
- how producer claim and publish differ
- how consumers register and report progress
- how `VarHandle` access modes replace simpler plain-buffer reads and writes
- where Java stays close to the C++ design and where it still pays JDK-level access overhead

## Per-Implementation Comparison

### Plain JDK: `java-jdk/src/SharedMemory.java`

Buffer stack:

- `MappedByteBuffer` as the shared-memory backing store
- `ByteBuffer.duplicate()` for bulk ring reads and writes
- heap `ByteBuffer` only in helper encode/decode methods

Hot-path behavior:

- producer reserves space, writes the payload directly into mapped storage, then publishes
- consumer spins on the sequence, reads ledger metadata, copies bytes out, then decodes

Strengths:

- simple
- direct write into final storage
- no generic queue framework overhead
- no per-message allocation on the optimized producer path

Weaknesses:

- the publication model is simple and not a true MPMC protocol
- plain mapped-buffer reads/writes do not express the full concurrency contract
- good as a baseline, but not the strongest semantic match for the C++ design

What the buffer choice implies:

- good for measuring the cost of a hand-rolled mapped-file format in Java
- not the best way to measure a fully correct Java MPMC publication protocol

### Agrona: `java-agrona/src/main/java/AgronaShm.java`

Buffer stack:

- `MappedByteBuffer`
- `UnsafeBuffer`
- `OneToOneRingBuffer`
- reusable `ExpandableArrayBuffer`

Hot-path behavior:

- producer encodes into the reusable staging buffer
- Agrona copies that data into the ring using its own record protocol
- consumer reads through Agrona’s ring-buffer scanning and callback flow

Strengths:

- very mature library code
- explicit ordering semantics inside the queue implementation
- low allocation pressure
- good real-world baseline for "library-managed off-heap messaging"

Weaknesses:

- extra payload copy versus direct-write designs
- record headers, alignment, and padding add structural overhead
- `OneToOneRingBuffer` is SPSC, so it is not a semantic clone of the custom MPMC design

What the buffer choice implies:

- often excellent throughput for SPSC workloads
- not an apples-to-apples concurrency comparison against the custom MPMC path
- useful when the priority is robust library behavior rather than exact protocol equivalence

### Custom MPMC: `java-mpmc/src/MpmcSharedMemory.java`

Buffer stack:

- `MappedByteBuffer`
- `VarHandle` byte-buffer views for atomic fields
- `ByteBuffer.duplicate()` views for wrapped bulk copies
- heap `ByteBuffer` only in helper decode code

Hot-path behavior:

- producer CAS-claims a sequence
- spins until the previous sequence is published
- writes ledger metadata and payload directly into final storage
- release-publishes the sequence
- consumer tracks its own next sequence and updates its registered slot

Strengths:

- closest Java match to the C++ protocol
- direct payload write into final storage
- explicit acquire/release/CAS semantics
- appropriate for many-producer style correctness

Weaknesses:

- more coordination overhead than the plain JDK path
- still limited by mapped-buffer access costs versus native pointers
- current startup path zero-fills the whole file, which can dominate one-shot timing

What the buffer choice implies:

- best representation of the cost of a custom Java MPMC mapped-file queue
- not necessarily the lowest apparent latency in trivial single-producer tests
- closer semantic fidelity than the other Java variants

## Heap Buffers Versus Off-Heap Buffers

The simplest split is:

- heap buffers are convenient and GC-managed
- off-heap buffers are closer to the actual shared-memory device

Heap-buffer advantages:

- simpler lifecycle
- easier API ergonomics
- fine for control-path helpers and testing

Heap-buffer disadvantages:

- allocation pressure if used per message
- extra copy if the final destination is a mapped file or native ring

Off-heap advantages:

- natural fit for shared memory
- avoids heap-copy staging when writing directly to the target region
- can reduce GC impact in sustained producer/consumer loops

Off-heap disadvantages:

- more manual correctness work
- memory ordering is easy to get wrong
- lifecycle, flushing, and mapping behavior are more operationally sensitive

## File-Backed `mmap` Versus SysV Shared Memory

The repo currently uses two different shared-memory mechanisms:

- the Java implementations use file-backed `MappedByteBuffer`
- the C++ implementation uses SysV shared memory via `ftok` / `shmget` / `shmat`

Both mechanisms expose a shared byte region to multiple processes, but they use different
transports and are not interchangeable.

### File-backed `mmap`

Mental model:

- a normal file is mapped into a process address space
- multiple processes that map the same file can see the same bytes

Typical flow:

1. create or open a file
2. size the file appropriately
3. map it into memory
4. read and write the mapped bytes directly
5. let other processes open and map the same file path

Why it is attractive:

- easy to use across languages
- easy to inspect operationally because the backing object is just a file
- straightforward to reopen and remap from a known filesystem path

### Does file-backed `mmap` really write to disk?

Potentially yes, but not in the simplistic sense of "every write becomes an immediate disk write."

What usually happens:

1. the process writes to the mapped region
2. the kernel marks those pages dirty in the page cache
3. the writes are visible in memory immediately to other processes mapping the same file
4. the kernel may flush the dirty pages to disk later

So the hot path is typically:

- memory access
- not a blocking file write syscall per message

File-backed `mmap` can still work well for shared-memory IPC because:

- writes are usually absorbed in RAM first
- the OS can batch writeback
- the steady-state producer path does not need a normal `write(...)` call per message

What adds cost or fragility:

- calling `force()` / `msync()` frequently
- a working set larger than available memory
- background writeback causing latency jitter
- using a real disk-backed filesystem when persistence is not actually needed

In practice, low-latency systems often avoid the "real disk" concern by mapping memory-backed
filesystems such as `/dev/shm` or by using a true shared-memory API instead of ordinary files.

### SysV shared memory

Mental model:

- the shared region is a kernel IPC object, not a normal file

Typical flow:

1. derive or choose a SysV IPC key
2. create or look up the segment with `shmget`
3. attach it into the process with `shmat`
4. let other processes attach using the same key
5. remove it explicitly with `shmctl(..., IPC_RMID, ...)`

Why it is attractive:

- no ordinary file needs to exist as the backing object
- the shared region is explicitly an IPC mechanism
- it avoids filesystem-path coordination for the data object itself

Tradeoffs:

- less convenient from Java without native bindings
- operational cleanup is done through IPC APIs rather than ordinary file lifecycle
- harder to inspect using normal filesystem tools

### Why the current C++ and Java implementations do not interoperate

This is the first compatibility blocker, before layout and atomics come into play.

The current implementations do different things:

- C++ creates and attaches a SysV shared-memory segment
- Java creates and maps a file

They are not pointing at the same backing object, even if the in-memory layout were identical.

So these pairings work:

- Java manager with the matching Java producer/consumer implementation
- C++ manager with the matching C++ producer/consumer implementation

But this pairing does not work with the current code:

- C++ manager plus Java publisher

Cross-language interop starts with the same transport on both sides:

- either both use file-backed `mmap`
- or both use SysV shared memory

Only after that does binary-layout compatibility matter.

## Why Performance Can Converge Anyway

The three Java implementations can end up closer than expected in steady state because:

- the message payload is tiny
- the memory is warm after startup
- repeated runs let the JIT optimize the hot loops
- all versions avoid per-message allocation on the critical path, or mostly do

The biggest differences usually come from:

- whether there is an extra payload copy
- whether the queue protocol adds headers, padding, or scanning
- whether the concurrency semantics require CAS and ordered publication
- whether startup work such as zero-filling is included in the measurement

## Practical Summary

If the goal is the simplest manual mapped-file baseline:

- use the plain JDK `MappedByteBuffer` version

If the goal is the best library-backed low-latency Java queue in this repo:

- use the Agrona version

If the goal is the closest Java semantic match to the custom C++ many-producer design:

- use the `VarHandle` MPMC version

The key point is that buffer type and queue semantics are coupled here:

- plain JDK buffer access is the simplest
- Agrona buffer access is the most library-optimized
- `VarHandle` over mapped bytes is the most explicit about concurrent publication

That is why two implementations can share the same underlying `MappedByteBuffer` and still have
meaningfully different performance and correctness characteristics.
