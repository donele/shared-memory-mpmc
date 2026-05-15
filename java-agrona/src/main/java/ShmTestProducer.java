public final class ShmTestProducer {
    private ShmTestProducer() {}

    public static void main(String[] args) throws Exception {
        final String name = args.length == 1 ? args[0] : "/tmp/shared-memory-mpmc-agrona-shm";

        try (AgronaShm shm = AgronaShm.attach(java.nio.file.Path.of(name))) {
            final int niter = 10_000;
            final int nruns = 5;
            final org.agrona.ExpandableArrayBuffer messageBuffer = AgronaShm.newMessageBuffer();

            for (int run = 0; run < nruns; ++run) {
                final long baseExchangeMicros = System.currentTimeMillis() * 1000L;
                final long begin = System.nanoTime();
                for (int i = 0; i < niter; ++i) {
                    shm.writeIncremental(messageBuffer, baseExchangeMicros + i, 1000L + i, 100L + i);
                }
                final long elapsed = System.nanoTime() - begin;
                System.out.println("Test producer run " + (run + 1) + "/" + nruns + ": "
                        + niter + " writes in " + elapsed
                        + " [ns], " + (elapsed / niter) + " ns per write");
            }
            System.out.println("Done test publisher");
        }
    }
}
