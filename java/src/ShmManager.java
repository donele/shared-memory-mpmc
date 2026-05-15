import java.nio.file.Path;

public final class ShmManager {
    private ShmManager() {}

    public static void main(String[] args) throws Exception {
        String name = args.length == 1 ? args[0] : "/tmp/shared-memory-mpmc-java-shm";
        Path path = Path.of(name);

        try (SharedMemory shm = SharedMemory.create(path, 1024L * 1024L * 1024L, 32, 1024)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    java.nio.file.Files.deleteIfExists(path);
                } catch (Exception ignored) {
                }
            }));

            System.out.println("Creating " + name + " " + (1024L * 1024L * 1024L));
            System.out.println("Initialized consumers=" + shm.getConsumerCount());
            while (true) {
                Thread.sleep(3000L);
            }
        }
    }
}
