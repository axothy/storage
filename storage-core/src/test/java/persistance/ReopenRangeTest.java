package persistance;

import org.junit.jupiter.api.Test;
import ru.axothy.api.Entry;
import ru.axothy.api.Storage;
import ru.axothy.storage.BaseEntry;
import utils.BaseTest;
import ru.axothy.storage.StorageFactory;

import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

public class ReopenRangeTest extends BaseTest {
    @Test
    void onlyPersistence() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entry("k1", "1"));
        storage.upsert(entry("k3", "3"));
        storage.upsert(entry("k2", "2"));
        storage.close();

        storage = StorageFactory.reopen(storage);
        storage.upsert(entry("k3", "5"));
        storage.upsert(entry("k2", "4"));
        storage.close();

        storage = StorageFactory.reopen(storage);

        assertSame(storage.all(),
                entry("k1", "1"),
                entry("k2", "4"),
                entry("k3", "5"));
    }

    @Test
    void onlyInMemory() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entry("k2", "2"));
        storage.upsert(entry("k3", "3"));
        storage.upsert(entry("k1", "1"));

        assertSame(storage.all(),
                entry("k1", "1"),
                entry("k2", "2"),
                entry("k3", "3"));
    }

    @Test
    void withInMemory() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entry("k1", "1"));
        storage.upsert(entry("k3", "3"));
        storage.upsert(entry("k2", "2"));
        storage.close();

        storage = StorageFactory.reopen(storage);
        storage.upsert(entry("k3", "5"));
        storage.upsert(entry("k2", "4"));
        storage.close();

        storage = StorageFactory.reopen(storage);
        storage.upsert(entry("k3", "6"));
        storage.upsert(entry("k4", "7"));

        assertSame(storage.all(),
                entry("k1", "1"),
                entry("k2", "4"),
                entry("k3", "6"),
                entry("k4", "7"));
    }

    @Test
    void allFromRange() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entry("k1", "1"));
        storage.upsert(entry("k3", "3"));
        storage.upsert(entry("k2", "2"));
        storage.close();

        storage = StorageFactory.reopen(storage);
        storage.upsert(entry("k3", "5"));
        storage.upsert(entry("k2", "4"));
        storage.close();

        storage = StorageFactory.reopen(storage);
        storage.upsert(entry("k3", "6"));
        storage.upsert(entry("k4", "7"));

        assertSame(storage.allFrom("k2"),
                entry("k2", "4"),
                entry("k3", "6"),
                entry("k4", "7"));
    }

    @Test
    void allToRange() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entry("k1", "1"));
        storage.upsert(entry("k3", "3"));
        storage.upsert(entry("k2", "2"));
        storage.close();

        storage = StorageFactory.reopen(storage);
        storage.upsert(entry("k3", "5"));
        storage.upsert(entry("k2", "4"));
        storage.close();

        storage = StorageFactory.reopen(storage);
        storage.upsert(entry("k3", "6"));
        storage.upsert(entry("k4", "7"));

        assertSame(storage.allTo("k4"),
                entry("k1", "1"),
                entry("k2", "4"),
                entry("k3", "6"));
    }

    @Test
    void rangeInTheMiddle() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entry("k1", "1"));
        storage.upsert(entry("k3", "3"));
        storage.upsert(entry("k2", "2"));
        storage.close();

        storage = StorageFactory.reopen(storage);
        storage.upsert(entry("k3", "5"));
        storage.upsert(entry("k2", "4"));
        storage.close();

        storage = StorageFactory.reopen(storage);
        storage.upsert(entry("k3", "6"));
        storage.upsert(entry("k4", "7"));

        assertSame(storage.get("k2", "k4"),
                entry("k2", "4"),
                entry("k3", "6"));
    }

    @Test
    void testDifferentFiles() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        List<Entry<String>> entries = entries(10_000);
        entries.forEach(storage::upsert);
        storage.close();

        storage = StorageFactory.reopen(storage);
        entries = entries(10_000, 5_000);
        entries.forEach(storage::upsert);
        storage.close();

        storage = StorageFactory.reopen(storage);

        List<Entry<String>> entries2 = new ArrayList<>(entries(5_000));
        entries2.addAll(entries);
        assertSame(storage.all(), entries2);
    }

    @Test
    void testManyFiles() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        int entriesPerFile = 1000;
        int numberOfFiles = 20;
        List<Entry<String>> entries;
        for (int i = 0; i < numberOfFiles / 2; i++) {
            entries = entries("k", "old", entriesPerFile, i * entriesPerFile);
            entries.forEach(storage::upsert);
            storage.close();
            storage = StorageFactory.reopen(storage);
        }

        for (int i = 0; i < numberOfFiles / 2; i++) {
            entries = entries("k", "new", entriesPerFile, i * entriesPerFile);
            entries.forEach(storage::upsert);
            storage.close();
            storage = StorageFactory.reopen(storage);
        }

        entries = entries("k", "new", entriesPerFile * numberOfFiles / 2);
        assertSame(storage.all(), entries);
    }

    public List<Entry<String>> entries(String keyPrefix, String valuePrefix, int count, int offset) {
        return new AbstractList<>() {
            @Override
            public Entry<String> get(int index) {
                checkInterrupted();
                if (index >= count || index < 0) {
                    throw new IndexOutOfBoundsException("Index is " + (index + offset) + ", size is " + count);
                }
                String paddedIdx = String.format("%010d", index + offset);
                String value = String.format("%010d", index + offset);
                return new BaseEntry<>(keyPrefix + paddedIdx, valuePrefix + value);
            }

            @Override
            public int size() {
                return count;
            }
        };
    }

    public List<Entry<String>> entries(int count, int offset) {
        return entries("k", "v", count, offset);
    }
}
