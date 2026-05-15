# shared-memory-mpmc

This repository contains:

- `cpp/`: minimal C++ subset copied from `sgt`
- `java-jdk/`: plain JDK mmap implementation
- `java-agrona/`: Java implementation using Agrona off-heap buffers and ring buffer primitives
- `java-mpmc/`: custom Java off-heap queue modeled after the C++ layout

## C++

### How to build

From the repository root:

```bash
cmake -S cpp -B cpp/build
cmake --build cpp/build -j
```

Build output:

- `cpp/build/shm_manager`
- `cpp/build/shm_test_producer`
- `cpp/build/shm_test_consumer_batch`

### How to run

Use a temporary shared-memory key path.

Start the manager:

```bash
./cpp/build/shm_manager /tmp/shared-memory-mpmc-shm
```

In a second terminal, start the consumer:

```bash
./cpp/build/shm_test_consumer_batch /tmp/shared-memory-mpmc-shm
```

In a third terminal, run the producer:

```bash
./cpp/build/shm_test_producer /tmp/shared-memory-mpmc-shm
```

The consumer prints the produced incremental messages.

### Producer benchmark

The producer benchmark runs 5 timed passes of 10,000 writes each.

Example result:

```text
Test producer run 1/5: 10000 writes in 2071226 [ns], 207 ns per write
Test producer run 2/5: 10000 writes in 2124977 [ns], 212 ns per write
Test producer run 3/5: 10000 writes in 2130648 [ns], 213 ns per write
Test producer run 4/5: 10000 writes in 2127348 [ns], 212 ns per write
Test producer run 5/5: 10000 writes in 2152304 [ns], 215 ns per write
```

## Java Versions

This repository includes three Java variants:

- `java-jdk/`: direct JDK implementation
- `java-agrona/`: Agrona-based implementation
- `java-mpmc/`: custom off-heap MPMC-style implementation

### Java JDK

#### How to build

From the repository root:

```bash
make -C java-jdk build
```

Build output:

- `java-jdk/build`

#### How to run

Start the manager:

```bash
java -cp java-jdk/build ShmManager /tmp/shared-memory-mpmc-java-shm
```

In a second terminal, start the consumer:

```bash
java -cp java-jdk/build ShmTestConsumerBatch /tmp/shared-memory-mpmc-java-shm
```

In a third terminal, run the producer:

```bash
java -cp java-jdk/build ShmTestProducer /tmp/shared-memory-mpmc-java-shm
```

The consumer prints the produced incremental messages.

#### Producer benchmark

The producer benchmark runs 5 timed passes of 10,000 writes each.

Example result:

```text
Test producer run 1/5: 10000 writes in 13484415 [ns], 1348 ns per write
Test producer run 2/5: 10000 writes in 2562240 [ns], 256 ns per write
Test producer run 3/5: 10000 writes in 2580213 [ns], 258 ns per write
Test producer run 4/5: 10000 writes in 2585456 [ns], 258 ns per write
Test producer run 5/5: 10000 writes in 2515965 [ns], 251 ns per write
```

### Java Agrona

#### How to build

Fetch the Agrona jar:

```bash
make -C java-agrona fetch-agrona
```

Build the sources:

```bash
make -C java-agrona build
```

Expected dependency path:

```bash
java-agrona/lib/agrona-1.22.0.jar
```

Build output:

- `java-agrona/build`

#### How to run

Start the manager:

```bash
java -cp 'java-agrona/build:java-agrona/lib/*' ShmManager /tmp/shared-memory-mpmc-agrona-shm
```

In a second terminal, start the consumer:

```bash
java -cp 'java-agrona/build:java-agrona/lib/*' ShmTestConsumerBatch /tmp/shared-memory-mpmc-agrona-shm
```

In a third terminal, run the producer:

```bash
java -cp 'java-agrona/build:java-agrona/lib/*' ShmTestProducer /tmp/shared-memory-mpmc-agrona-shm
```

The consumer prints the produced incremental messages.

#### Producer benchmark

The producer benchmark runs 5 timed passes of 10,000 writes each.

Example result:

```text
Test producer run 1/5: 10000 writes in 14065180 [ns], 1406 ns per write
Test producer run 2/5: 10000 writes in 1983073 [ns], 198 ns per write
Test producer run 3/5: 10000 writes in 3022645 [ns], 302 ns per write
Test producer run 4/5: 10000 writes in 1942122 [ns], 194 ns per write
Test producer run 5/5: 10000 writes in 1942282 [ns], 194 ns per write
```

### Java MPMC

#### How to build

From the repository root:

```bash
make -C java-mpmc build
```

Build output:

- `java-mpmc/build`

#### How to run

Start the manager:

```bash
java -cp java-mpmc/build ShmManager /tmp/shared-memory-mpmc-java-mpmc-shm
```

In a second terminal, start the consumer:

```bash
java -cp java-mpmc/build ShmTestConsumerBatch /tmp/shared-memory-mpmc-java-mpmc-shm
```

In a third terminal, run the producer:

```bash
java -cp java-mpmc/build ShmTestProducer /tmp/shared-memory-mpmc-java-mpmc-shm
```

The consumer prints the produced incremental messages.

#### Producer benchmark

The producer benchmark runs 5 timed passes of 10,000 writes each.

Example result:

```text
Test producer run 1/5: 10000 writes in 29654087 [ns], 2965 ns per write
Test producer run 2/5: 10000 writes in 2188581 [ns], 218 ns per write
Test producer run 3/5: 10000 writes in 2170471 [ns], 217 ns per write
Test producer run 4/5: 10000 writes in 2216077 [ns], 221 ns per write
Test producer run 5/5: 10000 writes in 2215846 [ns], 221 ns per write
```
