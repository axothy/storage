package ru.axothy.storage;

import ru.axothy.api.Storage;
import ru.axothy.api.Entry;
import ru.axothy.config.Config;
import ru.axothy.iterators.MergeIterator;
import ru.axothy.iterators.PeekingIterator;
import ru.axothy.iterators.PeekingIteratorImpl;
import ru.axothy.iterators.SkipTombstoneIterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static ru.axothy.storage.SSTableUtils.sizeOf;

public class LSMStorage implements Storage<MemorySegment, Entry<MemorySegment>> {

    private static final double BLOOM_FILTER_FPP = 0.03;

    private final SSTableManager ssTablesStorage;

    private final Config config;

    private final Arena arena;

    private final AtomicReference<StorageState> state;

    private final ExecutorService bgExecutor = Executors.newSingleThreadExecutor();

    private final AtomicBoolean closed = new AtomicBoolean();

    private final ReadWriteLock upsertLock = new ReentrantReadWriteLock();

    private final AtomicLong size = new AtomicLong();

    public LSMStorage(Config config) {
        this.config = config;
        this.arena = Arena.ofShared();
        this.ssTablesStorage = new SSTableManager(config.basePath());
        this.state = new AtomicReference<>(StorageState.initial(SSTableManager.loadOrRecover(config.basePath(), arena)));
    }

    public static int comparator(MemorySegment segment1, MemorySegment segment2) {
        long offset = segment1.mismatch(segment2);

        if (offset == -1) {
            return 0;
        }
        if (offset == segment1.byteSize()) {
            return -1;
        }
        if (offset == segment2.byteSize()) {
            return 1;
        }

        return Byte.compare(
                segment1.get(ValueLayout.JAVA_BYTE, offset),
                segment2.get(ValueLayout.JAVA_BYTE, offset)
        );
    }

    public static int entryComparator(Entry<MemorySegment> entry1, Entry<MemorySegment> entry2) {
        return comparator(entry1.key(), entry2.key());
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        StorageState currState = this.state.get();

        PeekingIterator<Entry<MemorySegment>> rangeIterator = range(
                memoryIterator(currState.getReadEntries(), from, to),
                memoryIterator(currState.getWriteEntries(), from, to),
                currState.getSstables(),
                from,
                to);

        return new SkipTombstoneIterator(rangeIterator);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        StorageState currState = this.state.get();

        Entry<MemorySegment> result = currState.getWriteEntries().get(key);
        if (result != null) {
            return result.value() == null ? null : result;
        }
        result = currState.getReadEntries().get(key);
        if (result != null) {
            return result.value() == null ? null : result;
        }

        return getFromDisk(key, currState);
    }

    private static Entry<MemorySegment> getFromDisk(MemorySegment key, StorageState state) {
        Entry<MemorySegment> result;

        for (MemorySegment sstable : state.getSstables()) {
            boolean mayContain = BloomFilter.sstableMayContain(key, sstable);
            if (mayContain) {
                result = SSTableUtils.get(sstable, key);

                if (result != null) {
                    return result.value() == null ? null : result;
                }
            }
        }

        return null;
    }

    private PeekingIterator<Entry<MemorySegment>> range(
            Iterator<Entry<MemorySegment>> firstIterator,
            Iterator<Entry<MemorySegment>> secondIterator,
            List<MemorySegment> segments,
            MemorySegment from,
            MemorySegment to
    ) {
        final List<PeekingIterator<Entry<MemorySegment>>> iterators = List.of(
                new PeekingIteratorImpl<>(firstIterator, 1),
                new PeekingIteratorImpl<>(secondIterator, 0),
                new PeekingIteratorImpl<>(SSTableManager.iteratorsAll(segments, from, to), 2)
        );

        return new PeekingIteratorImpl<>(MergeIterator.merge(iterators, LSMStorage::entryComparator));
    }

    private PeekingIterator<Entry<MemorySegment>> iteratorForCompaction(List<MemorySegment> segments) {
        return new PeekingIteratorImpl<>(SSTableManager.iteratorsAll(segments, null, null));
    }

    private static Iterator<Entry<MemorySegment>> memoryIterator(
            SortedMap<MemorySegment, Entry<MemorySegment>> entries,
            MemorySegment from,
            MemorySegment to
    ) {
        if (from == null && to == null) {
            return entries.values().iterator();
        } else if (from == null) {
            return entries.headMap(to).values().iterator();
        } else if (to == null) {
            return entries.tailMap(from).values().iterator();
        } else {
            return entries.subMap(from, to).values().iterator();
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        final boolean autoFlush;
        final MemorySegment key = entry.key();

        Entry<MemorySegment> old;
        upsertLock.readLock().lock();
        try {
            old = state.get().getWriteEntries().put(key, entry);

            final long curSize = size.addAndGet(sizeOf(entry) - sizeOf(old));
            autoFlush = curSize > config.flushThresholdBytes();
        } finally {
            upsertLock.readLock().unlock();
        }

        if (autoFlush) {
            flush();
        }
    }

    @Override
    public void compact() {
        bgExecutor.execute(() -> {
            try {
                StorageState currState = this.state.get();
                MemorySegment newPage = compact(currState.getSstables());
                this.state.set(currState.compact(newPage));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private MemorySegment compact(List<MemorySegment> segments) throws IOException {
        Iterator<Entry<MemorySegment>> iterator = new SkipTombstoneIterator(iteratorForCompaction(segments));

        long sizeForCompaction = 0;
        long entryCount = 0;
        long nonEmptyEntryCount = 0;
        while (iterator.hasNext()) {
            Entry<MemorySegment> entry = iterator.next();
            sizeForCompaction += entry.key().byteSize();
            MemorySegment value = entry.value();
            if (value != null) {
                sizeForCompaction += value.byteSize();
                nonEmptyEntryCount++;
            }
            entryCount++;
        }

        if (entryCount == 0) {
            return null;
        }

        long newBloomFilterLength = BloomFilter.bloomFilterLength(entryCount, BLOOM_FILTER_FPP);

        sizeForCompaction += 2L * Long.BYTES * nonEmptyEntryCount;
        sizeForCompaction += 3L * Long.BYTES + Long.BYTES * nonEmptyEntryCount; //for metadata (header + key offsets)
        sizeForCompaction += Long.BYTES * newBloomFilterLength; //for bloom filter

        iterator = new SkipTombstoneIterator(iteratorForCompaction(segments));
        return ssTablesStorage.compact(iterator, sizeForCompaction, nonEmptyEntryCount, newBloomFilterLength);
    }

    @Override
    public void flush() {
        bgExecutor.execute(() -> {
            StorageState prevState = state.get();
            SortedMap<MemorySegment, Entry<MemorySegment>> writeEntries = prevState.getWriteEntries();
            if (writeEntries.isEmpty()) {
                return;
            }

            StorageState nextState = prevState.beforeFlush();
            upsertLock.writeLock().lock();
            try {
                state.set(nextState);
            } finally {
                upsertLock.writeLock().unlock();
            }

            Collection<Entry<MemorySegment>> toFlush = writeEntries.values();;
            MemorySegment newPage;
            try {
                newPage = ssTablesStorage.write(toFlush, BLOOM_FILTER_FPP);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            nextState = nextState.afterFlush(newPage);
            upsertLock.writeLock().lock();
            try {
                state.set(nextState);
            } finally {
                upsertLock.writeLock().unlock();
            }
        });
    }

    @Override
    public void close() throws IOException {
        if (closed.getAndSet(true)) {
            waitForClose();
            return;
        }

        flush();
        bgExecutor.execute(arena::close);
        bgExecutor.shutdown();
        waitForClose();
    }

    private void waitForClose() {
        try {
            if (!bgExecutor.awaitTermination(5, TimeUnit.MINUTES)) {
                throw new InterruptedException("Timeout");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
