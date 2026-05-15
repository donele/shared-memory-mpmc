import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

final class AgronaShm implements AutoCloseable {
    static final int MSG_INCREMENTAL_L2 = 4;
    static final byte SIDE_BID = 0;
    static final int INCREMENTAL_SIZE = 2 + 8 + 8 + 8 + 8 + 1;
    static final int DEFAULT_CAPACITY = 1024 * 1024 * 64;

    private static final DateTimeFormatter TS_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC);

    private final Path path;
    private final FileChannel channel;
    private final MappedByteBuffer mappedByteBuffer;
    private final UnsafeBuffer atomicBuffer;
    private final OneToOneRingBuffer ringBuffer;

    private AgronaShm(
            Path path,
            FileChannel channel,
            MappedByteBuffer mappedByteBuffer,
            UnsafeBuffer atomicBuffer,
            OneToOneRingBuffer ringBuffer) {
        this.path = path;
        this.channel = channel;
        this.mappedByteBuffer = mappedByteBuffer;
        this.atomicBuffer = atomicBuffer;
        this.ringBuffer = ringBuffer;
    }

    static AgronaShm create(Path path, int capacity) throws IOException {
        if (Files.exists(path)) {
            throw new IOException("Shared memory file already exists: " + path);
        }

        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        final int actualCapacity = nextPowerOfTwo(capacity);
        final long fileSize = actualCapacity + RingBufferDescriptor.TRAILER_LENGTH;
        final FileChannel channel = FileChannel.open(
                path,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );
        channel.truncate(fileSize);
        final MappedByteBuffer mapped = channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
        zero(mapped, fileSize);

        final UnsafeBuffer buffer = new UnsafeBuffer(mapped);
        final OneToOneRingBuffer ring = new OneToOneRingBuffer(buffer);
        mapped.force();
        return new AgronaShm(path, channel, mapped, buffer, ring);
    }

    static AgronaShm attach(Path path) throws IOException {
        if (!Files.exists(path)) {
            throw new IOException("Shared memory file does not exist: " + path);
        }

        final FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        final long fileSize = channel.size();
        final MappedByteBuffer mapped = channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
        final UnsafeBuffer buffer = new UnsafeBuffer(mapped);
        final OneToOneRingBuffer ring = new OneToOneRingBuffer(buffer);
        return new AgronaShm(path, channel, mapped, buffer, ring);
    }

    OneToOneRingBuffer ringBuffer() {
        return ringBuffer;
    }

    void writeIncremental(MutableDirectBuffer messageBuffer, long exchangeTimeMicros, long price, long qty) {
        messageBuffer.putShort(0, (short) MSG_INCREMENTAL_L2);
        messageBuffer.putLong(2, exchangeTimeMicros);
        messageBuffer.putLong(10, price);
        messageBuffer.putLong(18, qty);
        messageBuffer.putLong(26, 0L);
        messageBuffer.putByte(34, SIDE_BID);

        while (!ringBuffer.write(MSG_INCREMENTAL_L2, messageBuffer, 0, INCREMENTAL_SIZE)) {
            Thread.onSpinWait();
        }
    }

    static IncrementalMessage readIncremental(org.agrona.DirectBuffer buffer, int index) {
        return new IncrementalMessage(
                buffer.getLong(index + 2),
                buffer.getLong(index + 10),
                buffer.getLong(index + 18),
                buffer.getLong(index + 26),
                buffer.getByte(index + 34)
        );
    }

    static ExpandableArrayBuffer newMessageBuffer() {
        return new ExpandableArrayBuffer(INCREMENTAL_SIZE);
    }

    static String formatMicros(long micros) {
        long seconds = micros / 1_000_000L;
        long microsPart = micros % 1_000_000L;
        return TS_FORMATTER.format(Instant.ofEpochSecond(seconds, microsPart * 1_000L));
    }

    static int nextPowerOfTwo(int value) {
        int out = 1;
        while (out < value) {
            out <<= 1;
        }
        return out;
    }

    private static void zero(MappedByteBuffer mapped, long size) {
        for (int i = 0; i < size; ++i) {
            mapped.put(i, (byte) 0);
        }
    }

    @Override
    public void close() throws IOException {
        mappedByteBuffer.force();
        channel.close();
    }

    static final class IncrementalMessage {
        final long exchangeTimeMicros;
        final long price;
        final long qty;
        final long numOrdersAtLevel;
        final byte side;

        IncrementalMessage(long exchangeTimeMicros, long price, long qty, long numOrdersAtLevel, byte side) {
            this.exchangeTimeMicros = exchangeTimeMicros;
            this.price = price;
            this.qty = qty;
            this.numOrdersAtLevel = numOrdersAtLevel;
            this.side = side;
        }

        @Override
        public String toString() {
            return "PacketType=IncrementalL2"
                    + " ExchangeTime=" + formatMicros(exchangeTimeMicros)
                    + " Price=" + price
                    + " Qty=" + qty
                    + " NumOrdersAtLevel=" + numOrdersAtLevel
                    + " Side=" + (side == SIDE_BID ? "BID" : "UNKNOWN");
        }
    }
}
