import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

final class SharedMemory implements AutoCloseable {
    static final short MSG_PACKET_HEADER = 0;
    static final short MSG_INCREMENTAL_L2 = 4;
    static final byte SIDE_BID = 0;
    static final byte SIDE_ASK = 1;

    private static final long MAGIC = 0x53484d4a41564131L; // SHMJAVA1
    private static final int VERSION = 1;

    private static final int CACHE_LINE_SIZE = 64;

    private static final int HEADER_MAGIC_OFFSET = 0;
    private static final int HEADER_VERSION_OFFSET = 8;
    private static final int HEADER_INITIALIZED_OFFSET = 12;
    private static final int HEADER_STORAGE_SIZE_OFFSET = 16;
    private static final int HEADER_LEDGER_SIZE_OFFSET = 24;
    private static final int HEADER_CONSUMERS_OFFSET = 32;
    private static final int HEADER_SEQUENCE_OFFSET = CACHE_LINE_SIZE;
    private static final int HEADER_CURRENT_OFFSET_OFFSET = CACHE_LINE_SIZE * 2;
    private static final int HEADER_SIZE = CACHE_LINE_SIZE * 3;

    private static final int LEDGER_ENTRY_SIZE = 24;

    private static final DateTimeFormatter TS_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC);

    private final Path path;
    private final FileChannel channel;
    private final MappedByteBuffer buffer;
    private long storageSize;
    private long ledgerSize;
    private int consumerCount;
    private long storageStart;

    private SharedMemory(Path path, FileChannel channel, MappedByteBuffer buffer) {
        this.path = path;
        this.channel = channel;
        this.buffer = buffer;
    }

    private void loadLayout() {
        this.storageSize = getLong(HEADER_STORAGE_SIZE_OFFSET);
        this.ledgerSize = getLong(HEADER_LEDGER_SIZE_OFFSET);
        this.consumerCount = getInt(HEADER_CONSUMERS_OFFSET);
        this.storageStart = HEADER_SIZE + ((long) consumerCount * 8L) + (ledgerSize * LEDGER_ENTRY_SIZE);
    }

    static SharedMemory create(Path path, long queueSize, int consumers, int objSizeHint) throws IOException {
        if (Files.exists(path)) {
            throw new IOException("Shared memory file already exists: " + path);
        }

        final Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        final long actualQueueSize = nextPowerOfTwo(queueSize);
        final long actualLedgerSize = actualQueueSize / nextPowerOfTwo(objSizeHint);
        final long totalSize = HEADER_SIZE + ((long) consumers * 8L)
                + (actualLedgerSize * LEDGER_ENTRY_SIZE) + actualQueueSize;

        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
            raf.setLength(totalSize);
        }

        final FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        final MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, totalSize);
        final SharedMemory shm = new SharedMemory(path, channel, buffer);
        shm.initialize(actualQueueSize, actualLedgerSize, consumers);
        shm.loadLayout();
        return shm;
    }

    static SharedMemory attach(Path path) throws IOException {
        if (!Files.exists(path)) {
            throw new IOException("Shared memory file does not exist: " + path);
        }
        final FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        final long size = channel.size();
        final MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, size);
        final SharedMemory shm = new SharedMemory(path, channel, buffer);
        shm.waitUntilInitialized();
        shm.loadLayout();
        return shm;
    }

    private void initialize(long actualQueueSize, long actualLedgerSize, int consumers) {
        putLong(HEADER_MAGIC_OFFSET, MAGIC);
        putInt(HEADER_VERSION_OFFSET, VERSION);
        putInt(HEADER_INITIALIZED_OFFSET, 0);
        putLong(HEADER_STORAGE_SIZE_OFFSET, actualQueueSize);
        putLong(HEADER_LEDGER_SIZE_OFFSET, actualLedgerSize);
        putInt(HEADER_CONSUMERS_OFFSET, consumers);
        putLong(HEADER_SEQUENCE_OFFSET, 0L);
        putLong(HEADER_CURRENT_OFFSET_OFFSET, 0L);
        putInt(HEADER_INITIALIZED_OFFSET, 1);
        buffer.force();
    }

    private void waitUntilInitialized() throws IOException {
        while (getInt(HEADER_INITIALIZED_OFFSET) != 1) {
            Thread.onSpinWait();
        }
        if (getLong(HEADER_MAGIC_OFFSET) != MAGIC) {
            throw new IOException("Invalid shared memory header in " + path);
        }
        if (getInt(HEADER_VERSION_OFFSET) != VERSION) {
            throw new IOException("Unsupported shared memory version in " + path);
        }
    }

    long getCursor() {
        return getLong(HEADER_SEQUENCE_OFFSET);
    }

    int getConsumerCount() {
        return consumerCount;
    }

    void produce(byte[] data) {
        final WriteReservation reservation = reserveWrite(data.length);
        writeStorage(reservation.offsetStart, data);
        finishWrite(reservation);
    }

    void produceIncremental(long exchangeTimeMicros, long price, long qty, long numOrdersAtLevel, byte side) {
        final int size = 2 + 8 + 8 + 8 + 8 + 1;
        final WriteReservation reservation = reserveWrite(size);
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

    BufferWrapper consume(long consumerSequence) {
        while (getLong(HEADER_SEQUENCE_OFFSET) < consumerSequence) {
            Thread.onSpinWait();
        }
        return readAtSequence(consumerSequence);
    }

    private BufferWrapper readAtSequence(long consumerSequence) {
        final long ledgerIndex = consumerSequence & (ledgerSize - 1L);
        final int ledgerOffset = ledgerOffset(ledgerIndex);
        final long offset = getLong(ledgerOffset);
        final int size = getInt(ledgerOffset + 8);
        return new BufferWrapper(readStorage(offset, size), size);
    }

    private WriteReservation reserveWrite(int size) {
        final long currentSequence = getLong(HEADER_SEQUENCE_OFFSET);
        final long desiredSequence = currentSequence + 1L;
        final long currentOffset = getLong(HEADER_CURRENT_OFFSET_OFFSET);
        final long remaining = storageSize - ((currentOffset & (storageSize - 1L)) + 1L);

        final long offsetStart;
        final long offsetEnd;
        if (size > remaining) {
            offsetStart = currentOffset + remaining + 1L;
            offsetEnd = offsetStart + size - 1L;
        } else {
            offsetStart = currentOffset + 1L;
            offsetEnd = offsetStart + size - 1L;
        }

        final long ledgerIndex = desiredSequence & (ledgerSize - 1L);
        final int ledgerOffset = ledgerOffset(ledgerIndex);
        putLong(ledgerOffset, offsetStart);
        putInt(ledgerOffset + 8, size);
        putShort(ledgerOffset + 12, (short) 0);
        putShort(ledgerOffset + 14, (short) 0);
        return new WriteReservation(desiredSequence, offsetEnd, offsetStart);
    }

    private void finishWrite(WriteReservation reservation) {
        putLong(HEADER_CURRENT_OFFSET_OFFSET, reservation.offsetEnd);
        putLong(HEADER_SEQUENCE_OFFSET, reservation.sequence);
    }

    private void writeShortToStorage(long offset, short value) {
        int storageIndex = (int) (offset & (storageSize - 1L));
        if (storageIndex + 2 <= storageSize) {
            buffer.putShort((int) (storageStart + storageIndex), value);
            return;
        }
        writeByteToStorage(offset, (byte) ((value >>> 8) & 0xff));
        writeByteToStorage(offset + 1, (byte) (value & 0xff));
    }

    private void writeLongToStorage(long offset, long value) {
        int storageIndex = (int) (offset & (storageSize - 1L));
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
        int storageIndex = (int) (offset & (storageSize - 1L));
        buffer.put((int) (storageStart + storageIndex), value);
    }

    private void writeStorage(long offsetStart, byte[] data) {
        int storageIndex = (int) (offsetStart & (storageSize - 1L));
        if (storageIndex + data.length <= storageSize) {
            final ByteBuffer view = buffer.duplicate();
            view.position((int) (storageStart + storageIndex));
            view.put(data);
            return;
        }

        int firstChunk = (int) (storageSize - storageIndex);
        final ByteBuffer view = buffer.duplicate();
        view.position((int) (storageStart + storageIndex));
        view.put(data, 0, firstChunk);
        view.position((int) storageStart);
        view.put(data, firstChunk, data.length - firstChunk);
    }

    private byte[] readStorage(long offsetStart, int size) {
        int storageIndex = (int) (offsetStart & (storageSize - 1L));
        byte[] out = new byte[size];
        if (storageIndex + size <= storageSize) {
            final ByteBuffer view = buffer.duplicate();
            view.position((int) (storageStart + storageIndex));
            view.get(out);
            return out;
        }

        int firstChunk = (int) (storageSize - storageIndex);
        final ByteBuffer view = buffer.duplicate();
        view.position((int) (storageStart + storageIndex));
        view.get(out, 0, firstChunk);
        view.position((int) storageStart);
        view.get(out, firstChunk, size - firstChunk);
        return out;
    }

    private int ledgerOffset(long ledgerIndex) {
        return (int) (HEADER_SIZE + ((long) consumerCount * 8L) + (ledgerIndex * LEDGER_ENTRY_SIZE));
    }

    private long getLong(int offset) {
        return buffer.getLong(offset);
    }

    private int getInt(int offset) {
        return buffer.getInt(offset);
    }

    private void putLong(int offset, long value) {
        buffer.putLong(offset, value);
    }

    private void putInt(int offset, int value) {
        buffer.putInt(offset, value);
    }

    private void putShort(int offset, short value) {
        buffer.putShort(offset, value);
    }

    static long nextPowerOfTwo(long value) {
        long out = 1L;
        while (out < value) {
            out <<= 1;
        }
        return out;
    }

    static byte[] encodeIncremental(long exchangeTimeMicros, long price, long qty, long numOrdersAtLevel, byte side) {
        ByteBuffer out = ByteBuffer.allocate(2 + 8 + 8 + 8 + 8 + 1);
        out.putShort(MSG_INCREMENTAL_L2);
        out.putLong(exchangeTimeMicros);
        out.putLong(price);
        out.putLong(qty);
        out.putLong(numOrdersAtLevel);
        out.put(side);
        return out.array();
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

    static short peekType(byte[] data) {
        return ByteBuffer.wrap(data).getShort();
    }

    static String formatMicros(long micros) {
        long seconds = micros / 1_000_000L;
        long microsPart = micros % 1_000_000L;
        return TS_FORMATTER.format(Instant.ofEpochSecond(seconds, microsPart * 1_000L));
    }

    static String sideToString(byte side) {
        if (side == SIDE_BID) {
            return "BID";
        }
        if (side == SIDE_ASK) {
            return "ASK";
        }
        return "UNKNOWN";
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    static final class BufferWrapper {
        final byte[] data;
        final int size;

        BufferWrapper(byte[] data, int size) {
            this.data = data;
            this.size = size;
        }
    }

    private static final class WriteReservation {
        final long sequence;
        final long offsetEnd;
        final long offsetStart;

        WriteReservation(long sequence, long offsetEnd, long offsetStart) {
            this.sequence = sequence;
            this.offsetEnd = offsetEnd;
            this.offsetStart = offsetStart;
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
                    + " Side=" + sideToString(side);
        }
    }
}
