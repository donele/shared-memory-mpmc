import java.nio.file.Files;
import java.nio.file.Path;

public final class ShmManager {
    private ShmManager() {}

    public static void main(String[] args) throws Exception {
        final String name = args.length == 1 ? args[0] : "/tmp/shared-memory-mpmc-agrona-shm";
        final Path path = Path.of(name);

        try (AgronaShm shm = AgronaShm.create(path, AgronaShm.DEFAULT_CAPACITY)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    Files.deleteIfExists(path);
                } catch (Exception ignored) {
                }
            }));

            System.out.println("Creating " + name + " " + AgronaShm.DEFAULT_CAPACITY);
            while (true) {
                Thread.sleep(3000L);
            }
        }
    }
}
