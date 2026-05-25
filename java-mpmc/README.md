# java-mpmc

Custom off-heap Java queue that follows the C++ queue structure more closely than the JDK and
Agrona variants.

## How to build

```bash
make build
```

## How to run

Start the manager:

```bash
java -cp build ShmManager /tmp/shared-memory-mpmc-java-mpmc-shm
```

Start the consumer:

```bash
java -cp build ShmTestConsumerBatch /tmp/shared-memory-mpmc-java-mpmc-shm
```

Run the producer:

```bash
java -cp build ShmTestProducer /tmp/shared-memory-mpmc-java-mpmc-shm
```
