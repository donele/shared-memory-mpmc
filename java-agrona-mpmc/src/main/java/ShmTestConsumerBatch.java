import java.nio.file.Path;

public final class ShmTestConsumerBatch {
    private ShmTestConsumerBatch() {}

    public static void main(String[] args) throws Exception {
        final String name = args.length == 1 ? args[0] : "/tmp/shared-memory-mpmc-java-agrona-mpmc-shm";
        System.out.println(name);

        try (AgronaMpmcShm shm = AgronaMpmcShm.attach(Path.of(name))) {
            final AgronaMpmcShm.ConsumerHandle consumer = shm.registerConsumer();
            try {
                while (true) {
                    final AgronaMpmcShm.MessageView view = shm.consumeNext(consumer);
                    final AgronaMpmcShm.IncrementalMessage msg = AgronaMpmcShm.decodeIncremental(view);
                    System.out.println("    [" + msg + "]");
                }
            } finally {
                shm.releaseConsumer(consumer);
            }
        }
    }
}
