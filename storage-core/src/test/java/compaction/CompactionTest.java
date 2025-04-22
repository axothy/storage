package compaction;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import ru.axothy.api.Entry;
import ru.axothy.api.Storage;
import utils.BaseTest;
import utils.StorageFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class CompactionTest extends BaseTest {
    @Test
    void empty() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        // Compact
        storage.compact();
        storage.close();

        // Check the contents
        storage = StorageFactory.reopen(storage);
        assertSame(storage.all(), new int[0]);
    }

    @Test
    void nothingToFlush() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        final Entry<String> entry = entryAt(42);
        storage.upsert(entry);

        // Compact and flush
        storage.compact();
        storage.close();

        // Check the contents
        storage = StorageFactory.reopen(storage);
        assertSame(storage.all(), entry);

        // Compact and flush
        storage.compact();
        storage.close();

        // Check the contents
        storage = StorageFactory.reopen(storage);
        assertSame(storage.all(), entry);
    }

    @Test
    @Timeout(value = 20)
    void overwrite() throws Exception {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        // Reference value
        int valueSize = 10 * 1024 * 1024;
        int keyCount = 3;
        int overwrites = 5;

        // 1 second should be enough to flush 10MB even to HDD
        Duration flushDelay = Duration.ofSeconds(1);

        List<Entry<String>> entries = bigValues(keyCount, valueSize);

        // Overwrite keys several times each time closing storage
        for (int round = 0; round < overwrites; round++) {
            for (Entry<String> entry : entries) {
                storage.upsert(entry);

                // Wait for a possible auto flush from stage 5
                Thread.sleep(flushDelay);
            }

            storage.close();
            storage = StorageFactory.reopen(storage);
        }

        // Big size
        storage.close();
        storage = StorageFactory.reopen(storage);
        long bigSize = sizePersistentData(storage);

        // Compact
        storage.compact();
        storage.close();

        // Check the contents
        storage = StorageFactory.reopen(storage);
        assertSame(storage.all(), entries);

        // Check store size
        long smallSize = sizePersistentData(storage);
        System.out.println(smallSize);
        System.out.println(bigSize);

        // Heuristic
        assertTrue(smallSize * (overwrites - 1) < bigSize);
        assertTrue(smallSize * (overwrites + 1) > bigSize);
    }

    @Test
    void multiple() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        // Reference value
        int valueSize = 1024 * 1024;
        int keyCount = 10;
        int overwrites = 10;

        List<Entry<String>> entries = bigValues(keyCount, valueSize);
        List<Long> sizes = new ArrayList<>();

        // Overwrite keys multiple times with intermediate compactions
        for (int round = 0; round < overwrites; round++) {
            // Overwrite
            for (Entry<String> entry : entries) {
                storage.upsert(entry);
            }

            // Compact
            storage.compact();
            storage.close();
            storage = StorageFactory.reopen(storage);
            sizes.add(sizePersistentData(storage));
        }

        LongSummaryStatistics stats = sizes.stream().mapToLong(k -> k).summaryStatistics();
        // Heuristic
        assertTrue(stats.getMax() - stats.getMin() < 1024);
    }

    @Test
    void compactAndAdd() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        List<Entry<String>> entries = entries(100);
        List<Entry<String>> firstHalf = entries.subList(0, 50);
        List<Entry<String>> lastHalf = entries.subList(50, 100);

        for (Entry<String> entry : firstHalf) {
            storage.upsert(entry);
        }
        storage.compact();
        storage.close();

        storage = StorageFactory.reopen(storage);
        for (Entry<String> entry : lastHalf) {
            storage.upsert(entry);
        }
        assertSame(storage.all(), entries);

        storage.flush();
        assertSame(storage.all(), entries);

        storage.close();
        storage = StorageFactory.reopen(storage);
        assertSame(storage.all(), entries);

        storage.compact();
        storage.close();
        storage = StorageFactory.reopen(storage);
        assertSame(storage.all(), entries);
    }

    @Disabled
    @Test
    void removeAllAndCompact() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        List<Entry<String>> entries = entries(100);
        for (Entry<String> entry : entries) {
            storage.upsert(entry);
        }
        storage.flush();
        assertSame(storage.all(), entries);
        storage.compact();
        storage.close();

        storage = StorageFactory.reopen(storage);
        assertSame(storage.all(), entries);

        // remove all
        for (int i = 0; i < entries.size(); i++) {
            storage.upsert(entry(keyAt(i), null));
        }
        // before compaction
        assertSame(storage.all(), new int[0]);
        // after flushing on disk
        storage.flush();
        assertSame(storage.all(), new int[0]);

        storage.compact();
        storage.close();

        storage = StorageFactory.reopen(storage);
        // after compaction
        assertSame(storage.all(), new int[0]);
    }

    @Test
    void mixedCompact() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        NavigableSet<Entry<String>> values = new TreeSet<>(Comparator.comparing(Entry::key));
        // insert some entries
        for (int i = 0; i < 50; i++) {
            values.add(entryAt(i));
            storage.upsert(entryAt(i));
        }

        // remove some entries
        for (int i = 0; i < 25; i++) {
            storage.upsert(entry(keyAt(i), null));
            values.remove(entryAt(i));
        }

        // insert more entries
        for (int i = 50; i < 100; i++) {
            values.add(entryAt(i));
            storage.upsert(entryAt(i));
        }

        assertSame(storage.all(), List.copyOf(values));

        storage.flush();
        assertSame(storage.all(), List.copyOf(values));

        storage.compact();
        storage.close();

        storage = StorageFactory.reopen(storage);

        assertSame(storage.all(), List.copyOf(values));
    }

    @Disabled
    @Test
    void addRemoveAddAndCompact() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        NavigableSet<Entry<String>> values = new TreeSet<>(Comparator.comparing(Entry::key));
        // insert some entries
        for (int i = 0; i < 50; i++) {
            values.add(entryAt(i));
            storage.upsert(entryAt(i));
        }

        // remove some entries
        for (int i = 0; i < 25; i++) {
            storage.upsert(entry(keyAt(i), null));
            values.remove(entryAt(i));
        }

        assertSame(storage.all(), List.copyOf(values));

        // flush and check
        storage.flush();
        assertSame(storage.all(), List.copyOf(values));

        // re-insert entries
        for (int i = 0; i < 25; i++) {
            values.add(entryAt(i));
            storage.upsert(entryAt(i));
        }

        assertSame(storage.all(), List.copyOf(values));

        // flush and check
        storage.flush();
        assertSame(storage.all(), List.copyOf(values));

        // compact and check
        storage.compact();
        storage.close();

        storage = StorageFactory.reopen(storage);

        assertSame(storage.all(), List.copyOf(values));
    }

}