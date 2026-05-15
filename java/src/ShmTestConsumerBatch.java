import java.nio.file.Path;

public final class ShmTestConsumerBatch {
    private ShmTestConsumerBatch() {}

    public static void main(String[] args) throws Exception {
        String name = args.length == 1 ? args[0] : "/tmp/shared-memory-mpmc-java-shm";
        System.out.println(name);

        try (SharedMemory shm = SharedMemory.attach(Path.of(name))) {
            long consumerSequence = shm.getCursor() + 1L;
            while (true) {
                SharedMemory.BufferWrapper buf = shm.consume(consumerSequence);
                short msgType = SharedMemory.peekType(buf.data);
                if (msgType == SharedMemory.MSG_INCREMENTAL_L2) {
                    SharedMemory.IncrementalMessage msg = SharedMemory.decodeIncremental(buf.data);
                    System.out.println("    [" + msg + "]");
                } else {
                    System.out.println("Unknown msg_type=" + msgType);
                }
                consumerSequence++;
            }
        }
    }
}
