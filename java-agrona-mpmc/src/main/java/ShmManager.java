import java.nio.file.Files;
import java.nio.file.Path;

public final class ShmManager {
    private ShmManager() {}

    public static void main(String[] args) throws Exception {
        final String name = args.length == 1 ? args[0] : "/tmp/shared-memory-mpmc-java-agrona-mpmc-shm";
        final Path path = Path.of(name);

        try (AgronaMpmcShm shm = AgronaMpmcShm.create(
                path,
                AgronaMpmcShm.DEFAULT_CAPACITY,
                AgronaMpmcShm.DEFAULT_CONSUMERS,
                AgronaMpmcShm.DEFAULT_OBJ_SIZE_HINT)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    Files.deleteIfExists(path);
                } catch (Exception ignored) {
                }
            }));

            System.out.println("Creating " + name + " " + AgronaMpmcShm.DEFAULT_CAPACITY);
            while (true) {
                Thread.sleep(3000L);
            }
        }
    }
}
