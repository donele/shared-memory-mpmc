import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

final class MpmcSharedMemory implements AutoCloseable {
    static final short MSG_INCREMENTAL_L2 = 4;
    static final byte SIDE_BID = 0;
    static final int INCREMENTAL_SIZE = 2 + 8 + 8 + 8 + 8 + 1;

    private static final long MAGIC = 0x4d504d434a323131L; // MPMCJ211
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

    private static final VarHandle LONG_HANDLE =
            MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle INT_HANDLE =
            MethodHandles.byteBufferViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);

    private static final DateTimeFormatter TS_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC);

    private final Path path;
    private final FileChannel channel;
    private final MappedByteBuffer buffer;
    private final long ledgerSize;
    private final long storageSize;
    private final int consumerCount;
    private final long consumerSlotsStart;
    private final long ledgerStart;
    private final long storageStart;

    private MpmcSharedMemory(
            Path path,
            FileChannel channel,
            MappedByteBuffer buffer,
            long ledgerSize,
            long storageSize,
            int consumerCount) {
        this.path = path;
        this.channel = channel;
        this.buffer = buffer;
        this.ledgerSize = ledgerSize;
        this.storageSize = storageSize;
        this.consumerCount = consumerCount;
        this.consumerSlotsStart = HEADER_SIZE;
        this.ledgerStart = consumerSlotsStart + ((long) consumerCount * CONSUMER_SLOT_SIZE);
        this.storageStart = ledgerStart + (ledgerSize * LEDGER_ENTRY_SIZE);
    }

    static MpmcSharedMemory create(Path path, long queueSize, int consumers, int objSizeHint) throws IOException {
        if (Files.exists(path)) {
            throw new IOException("Shared memory file already exists: " + path);
        }

        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        long actualQueueSize = nextPowerOfTwo(queueSize);
        long actualLedgerSize = actualQueueSize / nextPowerOfTwo(objSizeHint);
        long totalSize = HEADER_SIZE + ((long) consumers * CONSUMER_SLOT_SIZE)
                + (actualLedgerSize * LEDGER_ENTRY_SIZE) + actualQueueSize;

        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
            raf.setLength(totalSize);
        }

        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, totalSize);
        zero(buffer, totalSize);

        putLongPlain(buffer, HEADER_MAGIC_OFFSET, MAGIC);
        putIntPlain(buffer, HEADER_VERSION_OFFSET, VERSION);
        putIntPlain(buffer, HEADER_INITIALIZED_OFFSET, 0);
        putLongPlain(buffer, HEADER_LEDGER_SIZE_OFFSET, actualLedgerSize);
        putLongPlain(buffer, HEADER_STORAGE_SIZE_OFFSET, actualQueueSize);
        putLongPlain(buffer, HEADER_PUBLISHED_SEQUENCE_OFFSET, 0L);
        putLongPlain(buffer, HEADER_CLAIMED_SEQUENCE_OFFSET, 0L);
        putLongPlain(buffer, HEADER_CURRENT_OFFSET_OFFSET, 0L);
        putIntPlain(buffer, HEADER_CONSUMERS_OFFSET, consumers);
        setIntRelease(buffer, HEADER_INITIALIZED_OFFSET, 1);
        buffer.force();

        return new MpmcSharedMemory(path, channel, buffer, actualLedgerSize, actualQueueSize, consumers);
    }

    static MpmcSharedMemory attach(Path path) throws IOException {
        if (!Files.exists(path)) {
            throw new IOException("Shared memory file does not exist: " + path);
        }

        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        long size = channel.size();
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, size);
        while (getIntAcquire(buffer, HEADER_INITIALIZED_OFFSET) != 1) {
            Thread.onSpinWait();
        }
        if (getLongVolatile(buffer, HEADER_MAGIC_OFFSET) != MAGIC) {
            throw new IOException("Invalid shared memory magic in " + path);
        }
        if (getIntAcquire(buffer, HEADER_VERSION_OFFSET) != VERSION) {
            throw new IOException("Unsupported shared memory version in " + path);
        }

        long ledgerSize = getLongVolatile(buffer, HEADER_LEDGER_SIZE_OFFSET);
        long storageSize = getLongVolatile(buffer, HEADER_STORAGE_SIZE_OFFSET);
        int consumers = getIntAcquire(buffer, HEADER_CONSUMERS_OFFSET);
        return new MpmcSharedMemory(path, channel, buffer, ledgerSize, storageSize, consumers);
    }

    long getPublishedSequence() {
        return getLongVolatile(buffer, HEADER_PUBLISHED_SEQUENCE_OFFSET);
    }

    int getConsumerCount() {
        return consumerCount;
    }

    ConsumerHandle registerConsumer() {
        long startSequence = getPublishedSequence() + 1L;
        for (int i = 0; i < consumerCount; ++i) {
            int activeOffset = consumerActiveOffset(i);
            if ((boolean) INT_HANDLE.compareAndSet(buffer, activeOffset, 0, 1)) {
                putLongPlain(buffer, consumerSequenceOffset(i), startSequence);
                return new ConsumerHandle(i, startSequence);
            }
        }
        throw new IllegalStateException("No free consumer slot in " + path);
    }

    void releaseConsumer(ConsumerHandle consumer) {
        if (consumer.slotIndex >= 0) {
            putLongPlain(buffer, consumerSequenceOffset(consumer.slotIndex), 0L);
            setIntRelease(buffer, consumerActiveOffset(consumer.slotIndex), 0);
        }
    }

    void produceIncremental(long exchangeTimeMicros, long price, long qty, long numOrdersAtLevel, byte side) {
        WriteReservation reservation = reserveWrite(INCREMENTAL_SIZE, (short) 0, (short) 0);
        long offset = reservation.offsetStart;
        writeShortToStorage(offset, MSG_INCREMENTAL_L2);
        offset += 2;
        writeLongToStorage(offset, exchangeTimeMicros);
        offset += 8;
        writeLongToStorage(offset, price);
        offset += 8;
        writeLongToStorage(offset, qty);
        offset += 8;
        writeLongToStorage(offset, numOrdersAtLevel);
        offset += 8;
        writeByteToStorage(offset, side);
        finishWrite(reservation);
    }

    BufferWrapper consumeNext(ConsumerHandle consumer) {
        long targetSequence = consumer.nextSequence;
        while (getLongVolatile(buffer, HEADER_PUBLISHED_SEQUENCE_OFFSET) < targetSequence) {
            Thread.onSpinWait();
        }

        long ledgerIndex = targetSequence & (ledgerSize - 1L);
        int ledgerOffset = ledgerOffset(ledgerIndex);
        long offset = getLongVolatile(buffer, ledgerOffset + LEDGER_OFFSET_OFFSET);
        int size = getIntAcquire(buffer, ledgerOffset + LEDGER_SIZE_OFFSET);
        byte[] data = readStorage(offset, size);

        consumer.nextSequence = targetSequence + 1L;
        if (consumer.slotIndex >= 0) {
            setLongRelease(buffer, consumerSequenceOffset(consumer.slotIndex), consumer.nextSequence);
        }
        return new BufferWrapper(data, size);
    }

    private WriteReservation reserveWrite(int size, short topic, short strategyId) {
        long claimedExpected = getLongVolatile(buffer, HEADER_CLAIMED_SEQUENCE_OFFSET);
        long claimedDesired = claimedExpected + 1L;
        while (!(boolean) LONG_HANDLE.compareAndSet(
                buffer,
                HEADER_CLAIMED_SEQUENCE_OFFSET,
                claimedExpected,
                claimedDesired)) {
            claimedExpected = getLongVolatile(buffer, HEADER_CLAIMED_SEQUENCE_OFFSET);
            claimedDesired = claimedExpected + 1L;
        }

        long expectedPublished = claimedDesired - 1L;
        while (getLongVolatile(buffer, HEADER_PUBLISHED_SEQUENCE_OFFSET) != expectedPublished) {
            Thread.onSpinWait();
        }

        long currentOffset = getLongVolatile(buffer, HEADER_CURRENT_OFFSET_OFFSET);
        long remaining = storageSize - ((currentOffset & (storageSize - 1L)) + 1L);

        long offsetStart;
        long offsetEnd;
        if (size > remaining) {
            offsetStart = currentOffset + remaining + 1L;
            offsetEnd = offsetStart + size - 1L;
        } else {
            offsetStart = currentOffset + 1L;
            offsetEnd = offsetStart + size - 1L;
        }

        long ledgerIndex = claimedDesired & (ledgerSize - 1L);
        int ledgerOffset = ledgerOffset(ledgerIndex);
        putLongPlain(buffer, ledgerOffset + LEDGER_OFFSET_OFFSET, offsetStart);
        putIntPlain(buffer, ledgerOffset + LEDGER_SIZE_OFFSET, size);
        putShortPlain(buffer, ledgerOffset + LEDGER_TOPIC_OFFSET, topic);
        putShortPlain(buffer, ledgerOffset + LEDGER_STRATEGY_OFFSET, strategyId);

        return new WriteReservation(claimedDesired, offsetStart, offsetEnd);
    }

    private void finishWrite(WriteReservation reservation) {
        putLongPlain(buffer, HEADER_CURRENT_OFFSET_OFFSET, reservation.offsetEnd);
        setLongRelease(buffer, HEADER_PUBLISHED_SEQUENCE_OFFSET, reservation.sequence);
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

    private void writeShortToStorage(long offset, short value) {
        int storageIndex = storageIndex(offset);
        if (storageIndex + 2 <= storageSize) {
            buffer.putShort((int) (storageStart + storageIndex), value);
            return;
        }
        writeByteToStorage(offset, (byte) ((value >>> 8) & 0xff));
        writeByteToStorage(offset + 1, (byte) (value & 0xff));
    }

    private void writeLongToStorage(long offset, long value) {
        int storageIndex = storageIndex(offset);
        if (storageIndex + 8 <= storageSize) {
            buffer.putLong((int) (storageStart + storageIndex), value);
            return;
        }
        writeByteToStorage(offset, (byte) ((value >>> 56) & 0xff));
        writeByteToStorage(offset + 1, (byte) ((value >>> 48) & 0xff));
        writeByteToStorage(offset + 2, (byte) ((value >>> 40) & 0xff));
        writeByteToStorage(offset + 3, (byte) ((value >>> 32) & 0xff));
        writeByteToStorage(offset + 4, (byte) ((value >>> 24) & 0xff));
        writeByteToStorage(offset + 5, (byte) ((value >>> 16) & 0xff));
        writeByteToStorage(offset + 6, (byte) ((value >>> 8) & 0xff));
        writeByteToStorage(offset + 7, (byte) (value & 0xff));
    }

    private void writeByteToStorage(long offset, byte value) {
        buffer.put((int) (storageStart + storageIndex(offset)), value);
    }

    private byte[] readStorage(long offsetStart, int size) {
        int storageIndex = storageIndex(offsetStart);
        byte[] out = new byte[size];
        if (storageIndex + size <= storageSize) {
            ByteBuffer view = buffer.duplicate();
            view.position((int) (storageStart + storageIndex));
            view.get(out);
            return out;
        }

        int firstChunk = (int) (storageSize - storageIndex);
        ByteBuffer view = buffer.duplicate();
        view.position((int) (storageStart + storageIndex));
        view.get(out, 0, firstChunk);
        view.position((int) storageStart);
        view.get(out, firstChunk, size - firstChunk);
        return out;
    }

    private int storageIndex(long offset) {
        return (int) (offset & (storageSize - 1L));
    }

    static IncrementalMessage decodeIncremental(byte[] data) {
        ByteBuffer in = ByteBuffer.wrap(data);
        short type = in.getShort();
        if (type != MSG_INCREMENTAL_L2) {
            throw new IllegalArgumentException("Unexpected msg_type=" + type);
        }
        return new IncrementalMessage(
                in.getLong(),
                in.getLong(),
                in.getLong(),
                in.getLong(),
                in.get()
        );
    }

    static String formatMicros(long micros) {
        long seconds = micros / 1_000_000L;
        long microsPart = micros % 1_000_000L;
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

    private static long getLongVolatile(ByteBuffer buffer, int offset) {
        return (long) LONG_HANDLE.getVolatile(buffer, offset);
    }

    private static int getIntAcquire(ByteBuffer buffer, int offset) {
        return (int) INT_HANDLE.getAcquire(buffer, offset);
    }

    private static void setLongRelease(ByteBuffer buffer, int offset, long value) {
        LONG_HANDLE.setRelease(buffer, offset, value);
    }

    private static void setIntRelease(ByteBuffer buffer, int offset, int value) {
        INT_HANDLE.setRelease(buffer, offset, value);
    }

    private static void putLongPlain(ByteBuffer buffer, int offset, long value) {
        buffer.putLong(offset, value);
    }

    private static void putIntPlain(ByteBuffer buffer, int offset, int value) {
        buffer.putInt(offset, value);
    }

    private static void putShortPlain(ByteBuffer buffer, int offset, short value) {
        buffer.putShort(offset, value);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    static final class ConsumerHandle {
        final int slotIndex;
        long nextSequence;

        ConsumerHandle(int slotIndex, long nextSequence) {
            this.slotIndex = slotIndex;
            this.nextSequence = nextSequence;
        }
    }

    static final class BufferWrapper {
        final byte[] data;
        final int size;

        BufferWrapper(byte[] data, int size) {
            this.data = data;
            this.size = size;
        }
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
