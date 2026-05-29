import java.nio.file.Path;

public final class ShmTestConsumerBatch {
    private ShmTestConsumerBatch() {}

    public static void main(String[] args) throws Exception {
        String name = args.length == 1 ? args[0] : "/tmp/shared-memory-mpmc-java-mpmc-shm";
        System.out.println(name);

        try (MpmcSharedMemory shm = MpmcSharedMemory.attach(Path.of(name))) {
            MpmcSharedMemory.ConsumerHandle consumer = shm.registerConsumer();
            try {
                while (true) {
                    MpmcSharedMemory.BufferWrapper buf = shm.consumeNext(consumer);
                    MpmcSharedMemory.IncrementalMessage msg = MpmcSharedMemory.decodeIncremental(buf);
                    System.out.println("    [" + msg + "]");
                }
            } finally {
                shm.releaseConsumer(consumer);
            }
        }
    }
}
