# java-agrona-mpmc

Multi-process MPMC shared-memory queue built on Agrona buffer primitives.

The queue uses:

- file-backed shared memory through `MappedByteBuffer`
- Agrona `UnsafeBuffer` for atomic and direct buffer access
- direct decode from mapped storage without copying payload bytes into a local array

## How to build

```bash
make build
```

## How to run

Start the manager:

```bash
java -cp "build:../java-agrona/lib/agrona-1.22.0.jar" ShmManager /tmp/shared-memory-mpmc-java-agrona-mpmc-shm
```

Start one or more consumers:

```bash
java -cp "build:../java-agrona/lib/agrona-1.22.0.jar" ShmTestConsumerBatch /tmp/shared-memory-mpmc-java-agrona-mpmc-shm
```

Run one or more producers:

```bash
java -cp "build:../java-agrona/lib/agrona-1.22.0.jar" ShmTestProducer /tmp/shared-memory-mpmc-java-agrona-mpmc-shm
```
