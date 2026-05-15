import org.agrona.concurrent.MessageHandler;

public final class ShmTestConsumerBatch {
    private ShmTestConsumerBatch() {}

    public static void main(String[] args) throws Exception {
        final String name = args.length == 1 ? args[0] : "/tmp/shared-memory-mpmc-agrona-shm";
        System.out.println(name);

        try (AgronaShm shm = AgronaShm.attach(java.nio.file.Path.of(name))) {
            final MessageHandler handler = (msgTypeId, buffer, index, length) -> {
                if (msgTypeId != AgronaShm.MSG_INCREMENTAL_L2) {
                    System.out.println("Unknown msg_type=" + msgTypeId);
                    return;
                }

                final AgronaShm.IncrementalMessage msg = AgronaShm.readIncremental(buffer, index);
                System.out.println("    [" + msg + "]");
            };

            while (true) {
                final int read = shm.ringBuffer().read(handler, 256);
                if (read == 0) {
                    Thread.onSpinWait();
                }
            }
        }
    }
}
