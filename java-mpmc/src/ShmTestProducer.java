import java.nio.file.Path;

public final class ShmTestProducer {
    private ShmTestProducer() {}

    public static void main(String[] args) throws Exception {
        String name = args.length == 1 ? args[0] : "/tmp/shared-memory-mpmc-java-mpmc-shm";

        try (MpmcSharedMemory shm = MpmcSharedMemory.attach(Path.of(name))) {
            int niter = 10_000;
            int nruns = 5;
            for (int run = 0; run < nruns; ++run) {
                long baseExchangeMicros = System.currentTimeMillis() * 1000L;
                long begin = System.nanoTime();
                for (int i = 0; i < niter; ++i) {
                    shm.produceIncremental(
                            baseExchangeMicros + i,
                            1000L + i,
                            100L + i,
                            0L,
                            MpmcSharedMemory.SIDE_BID
                    );
                }
                long elapsed = System.nanoTime() - begin;
                System.out.println("Test producer run " + (run + 1) + "/" + nruns + ": "
                        + niter + " writes in " + elapsed
                        + " [ns], " + (elapsed / niter) + " ns per write");
            }
            System.out.println("Done test publisher");
        }
    }
}
