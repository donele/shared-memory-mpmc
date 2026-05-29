import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

final class AgronaMpmcShm implements AutoCloseable {
    static final short MSG_INCREMENTAL_L2 = 4;
    static final byte SIDE_BID = 0;
    static final int INCREMENTAL_SIZE = 2 + 8 + 8 + 8 + 8 + 1;
    static final long DEFAULT_CAPACITY = 1024L * 1024L * 1024L;
    static final int DEFAULT_CONSUMERS = 32;
    static final int DEFAULT_OBJ_SIZE_HINT = 1024;

    private static final long MAGIC = 0x4147524d504d4331L; // AGRMPMC1
    private static final int VERSION = 1;

    private static final int CACHE_LINE_SIZE = 64;

    private static final int HEADER_MAGIC_OFFSET = 0;
    private static final int HEADER_VERSION_OFFSET = 8;
    private static final int HEADER_INITIALIZED_OFFSET = 12;
    private static final int HEADER_LEDGER_SIZE_OFFSET = 16;
    private static final int HEADER_STORAGE_SIZE_OFFSET = 24;
    private static final int HEADER_CONSUMERS_OFFSET = 32;
    private static final int HEADER_PUBLISHED_SEQUENCE_OFFSET = CACHE_LINE_SIZE;
    private static final int HEADER_CLAIMED_SEQUENCE_OFFSET = CACHE_LINE_SIZE * 2;
    private static final int HEADER_CURRENT_OFFSET_OFFSET = CACHE_LINE_SIZE * 3;
    private static final int HEADER_SIZE = CACHE_LINE_SIZE * 4;

    private static final int CONSUMER_SLOT_SIZE = 16;
    private static final int CONSUMER_SEQUENCE_OFFSET = 0;
    private static final int CONSUMER_ACTIVE_OFFSET = 8;

    private static final int LEDGER_ENTRY_SIZE = 24;
    private static final int LEDGER_OFFSET_OFFSET = 0;
    private static final int LEDGER_SIZE_OFFSET = 8;
    private static final int LEDGER_TOPIC_OFFSET = 12;
    private static final int LEDGER_STRATEGY_OFFSET = 14;

    private static final DateTimeFormatter TS_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC);

    private final Path path;
    private final FileChannel channel;
    private final MappedByteBuffer mappedByteBuffer;
    private final UnsafeBuffer atomicBuffer;
    private final long ledgerSize;
    private final long storageSize;
    private final int consumerCount;
    private final long consumerSlotsStart;
    private final long ledgerStart;
    private final long storageStart;

    private AgronaMpmcShm(
            Path path,
            FileChannel channel,
            MappedByteBuffer mappedByteBuffer,
            UnsafeBuffer atomicBuffer,
            long ledgerSize,
            long storageSize,
            int consumerCount) {
        this.path = path;
        this.channel = channel;
        this.mappedByteBuffer = mappedByteBuffer;
        this.atomicBuffer = atomicBuffer;
        this.ledgerSize = ledgerSize;
        this.storageSize = storageSize;
        this.consumerCount = consumerCount;
        this.consumerSlotsStart = HEADER_SIZE;
        this.ledgerStart = consumerSlotsStart + ((long) consumerCount * CONSUMER_SLOT_SIZE);
        this.storageStart = ledgerStart + (ledgerSize * LEDGER_ENTRY_SIZE);
    }

    static AgronaMpmcShm create(Path path, long queueSize, int consumers, int objSizeHint) throws IOException {
        if (Files.exists(path)) {
            throw new IOException("Shared memory file already exists: " + path);
        }

        final Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        final long actualQueueSize = nextPowerOfTwo(queueSize);
        final long actualLedgerSize = actualQueueSize / nextPowerOfTwo(objSizeHint);
        final long totalSize = HEADER_SIZE + ((long) consumers * CONSUMER_SLOT_SIZE)
                + (actualLedgerSize * LEDGER_ENTRY_SIZE) + actualQueueSize;

        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
            raf.setLength(totalSize);
        }

        final FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        final MappedByteBuffer mapped = channel.map(FileChannel.MapMode.READ_WRITE, 0, totalSize);
        zero(mapped, totalSize);

        final UnsafeBuffer buffer = new UnsafeBuffer(mapped);
        buffer.putLong(HEADER_MAGIC_OFFSET, MAGIC);
        buffer.putInt(HEADER_VERSION_OFFSET, VERSION);
        buffer.putInt(HEADER_INITIALIZED_OFFSET, 0);
        buffer.putLong(HEADER_LEDGER_SIZE_OFFSET, actualLedgerSize);
        buffer.putLong(HEADER_STORAGE_SIZE_OFFSET, actualQueueSize);
        buffer.putLong(HEADER_PUBLISHED_SEQUENCE_OFFSET, 0L);
        buffer.putLong(HEADER_CLAIMED_SEQUENCE_OFFSET, 0L);
        buffer.putLong(HEADER_CURRENT_OFFSET_OFFSET, -1L);
        buffer.putInt(HEADER_CONSUMERS_OFFSET, consumers);
        buffer.putIntOrdered(HEADER_INITIALIZED_OFFSET, 1);
        mapped.force();

        return new AgronaMpmcShm(path, channel, mapped, buffer, actualLedgerSize, actualQueueSize, consumers);
    }

    static AgronaMpmcShm attach(Path path) throws IOException {
        if (!Files.exists(path)) {
            throw new IOException("Shared memory file does not exist: " + path);
        }

        final FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        final long fileSize = channel.size();
        final MappedByteBuffer mapped = channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
        final UnsafeBuffer buffer = new UnsafeBuffer(mapped);

        while (buffer.getIntVolatile(HEADER_INITIALIZED_OFFSET) != 1) {
            Thread.onSpinWait();
        }

        if (buffer.getLongVolatile(HEADER_MAGIC_OFFSET) != MAGIC) {
            throw new IOException("Invalid shared memory magic in " + path);
        }
        if (buffer.getIntVolatile(HEADER_VERSION_OFFSET) != VERSION) {
            throw new IOException("Unsupported shared memory version in " + path);
        }

        final long ledgerSize = buffer.getLongVolatile(HEADER_LEDGER_SIZE_OFFSET);
        final long storageSize = buffer.getLongVolatile(HEADER_STORAGE_SIZE_OFFSET);
        final int consumers = buffer.getIntVolatile(HEADER_CONSUMERS_OFFSET);
        return new AgronaMpmcShm(path, channel, mapped, buffer, ledgerSize, storageSize, consumers);
    }

    long getPublishedSequence() {
        return atomicBuffer.getLongVolatile(HEADER_PUBLISHED_SEQUENCE_OFFSET);
    }

    ConsumerHandle registerConsumer() {
        final long startSequence = getPublishedSequence() + 1L;
        for (int i = 0; i < consumerCount; ++i) {
            final int activeOffset = consumerActiveOffset(i);
            if (atomicBuffer.compareAndSetInt(activeOffset, 0, 1)) {
                atomicBuffer.putLong(consumerSequenceOffset(i), startSequence);
                return new ConsumerHandle(i, startSequence);
            }
        }

        throw new IllegalStateException("No free consumer slot in " + path);
    }

    void releaseConsumer(ConsumerHandle consumer) {
        if (consumer.slotIndex >= 0) {
            atomicBuffer.putLong(consumerSequenceOffset(consumer.slotIndex), 0L);
            atomicBuffer.putIntOrdered(consumerActiveOffset(consumer.slotIndex), 0);
        }
    }

    void produceIncremental(long exchangeTimeMicros, long price, long qty, long numOrdersAtLevel, byte side) {
        final WriteReservation reservation = reserveWrite(INCREMENTAL_SIZE, (short) 0, (short) 0);
        int offset = storageOffset(reservation.offsetStart);
        atomicBuffer.putShort(offset, MSG_INCREMENTAL_L2);
        offset += 2;
        atomicBuffer.putLong(offset, exchangeTimeMicros);
        offset += 8;
        atomicBuffer.putLong(offset, price);
        offset += 8;
        atomicBuffer.putLong(offset, qty);
        offset += 8;
        atomicBuffer.putLong(offset, numOrdersAtLevel);
        offset += 8;
        atomicBuffer.putByte(offset, side);
        finishWrite(reservation);
    }

    MessageView consumeNext(ConsumerHandle consumer) {
        final long targetSequence = consumer.nextSequence;
        while (atomicBuffer.getLongVolatile(HEADER_PUBLISHED_SEQUENCE_OFFSET) < targetSequence) {
            Thread.onSpinWait();
        }

        final long ledgerIndex = targetSequence & (ledgerSize - 1L);
        final int ledgerOffset = ledgerOffset(ledgerIndex);
        final long offset = atomicBuffer.getLongVolatile(ledgerOffset + LEDGER_OFFSET_OFFSET);
        final int size = atomicBuffer.getIntVolatile(ledgerOffset + LEDGER_SIZE_OFFSET);

        final MessageView messageView = consumer.messageView;
        messageView.buffer.wrap(atomicBuffer, storageOffset(offset), size);
        messageView.size = size;

        consumer.nextSequence = targetSequence + 1L;
        if (consumer.slotIndex >= 0) {
            atomicBuffer.putLongOrdered(consumerSequenceOffset(consumer.slotIndex), consumer.nextSequence);
        }
        return messageView;
    }

    private WriteReservation reserveWrite(int size, short topic, short strategyId) {
        if (size > storageSize) {
            throw new IllegalArgumentException("Message size " + size + " exceeds storage size " + storageSize);
        }

        long claimedExpected = atomicBuffer.getLongVolatile(HEADER_CLAIMED_SEQUENCE_OFFSET);
        long claimedDesired = claimedExpected + 1L;
        while (!atomicBuffer.compareAndSetLong(HEADER_CLAIMED_SEQUENCE_OFFSET, claimedExpected, claimedDesired)) {
            claimedExpected = atomicBuffer.getLongVolatile(HEADER_CLAIMED_SEQUENCE_OFFSET);
            claimedDesired = claimedExpected + 1L;
        }

        final long expectedPublished = claimedDesired - 1L;
        while (atomicBuffer.getLongVolatile(HEADER_PUBLISHED_SEQUENCE_OFFSET) != expectedPublished) {
            Thread.onSpinWait();
        }

        final long currentOffset = atomicBuffer.getLongVolatile(HEADER_CURRENT_OFFSET_OFFSET);
        final int currentIndex = storageIndex(currentOffset);
        final long remaining = storageSize - (currentIndex + 1L);

        final long offsetStart;
        final long offsetEnd;
        if (size > remaining) {
            offsetStart = currentOffset + remaining + 1L;
            offsetEnd = offsetStart + size - 1L;
        } else {
            offsetStart = currentOffset + 1L;
            offsetEnd = offsetStart + size - 1L;
        }

        final long ledgerIndex = claimedDesired & (ledgerSize - 1L);
        final int ledgerOffset = ledgerOffset(ledgerIndex);
        atomicBuffer.putLong(ledgerOffset + LEDGER_OFFSET_OFFSET, offsetStart);
        atomicBuffer.putInt(ledgerOffset + LEDGER_SIZE_OFFSET, size);
        atomicBuffer.putShort(ledgerOffset + LEDGER_TOPIC_OFFSET, topic);
        atomicBuffer.putShort(ledgerOffset + LEDGER_STRATEGY_OFFSET, strategyId);

        return new WriteReservation(claimedDesired, offsetStart, offsetEnd);
    }

    private void finishWrite(WriteReservation reservation) {
        atomicBuffer.putLong(HEADER_CURRENT_OFFSET_OFFSET, reservation.offsetEnd);
        atomicBuffer.putLongOrdered(HEADER_PUBLISHED_SEQUENCE_OFFSET, reservation.sequence);
    }

    static IncrementalMessage decodeIncremental(DirectBuffer buffer, int offset) {
        final short type = buffer.getShort(offset);
        if (type != MSG_INCREMENTAL_L2) {
            throw new IllegalArgumentException("Unexpected msg_type=" + type);
        }

        return new IncrementalMessage(
                buffer.getLong(offset + 2),
                buffer.getLong(offset + 10),
                buffer.getLong(offset + 18),
                buffer.getLong(offset + 26),
                buffer.getByte(offset + 34)
        );
    }

    static IncrementalMessage decodeIncremental(MessageView messageView) {
        return decodeIncremental(messageView.buffer, 0);
    }

    private int ledgerOffset(long ledgerIndex) {
        return (int) (ledgerStart + (ledgerIndex * LEDGER_ENTRY_SIZE));
    }

    private int consumerSequenceOffset(int slotIndex) {
        return (int) (consumerSlotsStart + ((long) slotIndex * CONSUMER_SLOT_SIZE) + CONSUMER_SEQUENCE_OFFSET);
    }

    private int consumerActiveOffset(int slotIndex) {
        return (int) (consumerSlotsStart + ((long) slotIndex * CONSUMER_SLOT_SIZE) + CONSUMER_ACTIVE_OFFSET);
    }

    private int storageIndex(long offset) {
        return (int) (offset & (storageSize - 1L));
    }

    private int storageOffset(long offset) {
        return (int) (storageStart + storageIndex(offset));
    }

    static String formatMicros(long micros) {
        final long seconds = micros / 1_000_000L;
        final long microsPart = micros % 1_000_000L;
        return TS_FORMATTER.format(Instant.ofEpochSecond(seconds, microsPart * 1_000L));
    }

    static long nextPowerOfTwo(long value) {
        long out = 1L;
        while (out < value) {
            out <<= 1;
        }
        return out;
    }

    private static void zero(MappedByteBuffer buffer, long size) {
        for (int i = 0; i < size; ++i) {
            buffer.put(i, (byte) 0);
        }
    }

    @Override
    public void close() throws IOException {
        mappedByteBuffer.force();
        channel.close();
    }

    static final class ConsumerHandle {
        final int slotIndex;
        long nextSequence;
        final MessageView messageView = new MessageView();

        ConsumerHandle(int slotIndex, long nextSequence) {
            this.slotIndex = slotIndex;
            this.nextSequence = nextSequence;
        }
    }

    static final class MessageView {
        final UnsafeBuffer buffer = new UnsafeBuffer();
        int size;
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

    private static final class WriteReservation {
        final long sequence;
        final long offsetStart;
        final long offsetEnd;

        WriteReservation(long sequence, long offsetStart, long offsetEnd) {
            this.sequence = sequence;
            this.offsetStart = offsetStart;
            this.offsetEnd = offsetEnd;
        }
    }
}
