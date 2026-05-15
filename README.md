# shared-memory-mpmc

This repository contains a minimal C++ subset under `cpp/` copied from `sgt` to run:

- `shm_manager`
- `shm_test_producer`
- `shm_test_consumer_batch`

It also contains a Java version under `java/` with the same manager / producer / consumer test flow.

## Build

From the repository root:

```bash
cmake -S cpp -B cpp/build
cmake --build cpp/build -j
```

The binaries will be written to:

- `cpp/build/shm_manager`
- `cpp/build/shm_test_producer`
- `cpp/build/shm_test_consumer_batch`

## Smoke Test

Use a temporary shared-memory key path:

```bash
./cpp/build/shm_manager /tmp/shared-memory-mpmc-shm
```

In a second terminal, start the consumer first:

```bash
./cpp/build/shm_test_consumer_batch /tmp/shared-memory-mpmc-shm
```

In a third terminal, run the producer:

```bash
./cpp/build/shm_test_producer /tmp/shared-memory-mpmc-shm
```

The consumer should print the produced incremental messages.

## Java Build

From the repository root:

```bash
make -C java build
```

The compiled classes will be written to `java/build`.

## Java Smoke Test

Use a temporary mmap file path:

```bash
java -cp java/build ShmManager /tmp/shared-memory-mpmc-java-shm
```

In a second terminal, start the consumer first:

```bash
java -cp java/build ShmTestConsumerBatch /tmp/shared-memory-mpmc-java-shm
```

In a third terminal, run the producer:

```bash
java -cp java/build ShmTestProducer /tmp/shared-memory-mpmc-java-shm
```

The Java consumer should print the produced incremental messages.
