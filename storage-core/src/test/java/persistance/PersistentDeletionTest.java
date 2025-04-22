package persistance;

import org.junit.jupiter.api.Test;
import ru.axothy.api.Entry;
import ru.axothy.api.Storage;
import utils.BaseTest;
import utils.StorageFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertFalse;

public class PersistentDeletionTest extends BaseTest {

    @Test
    void deleteOldValueFromFile() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entry(keyAt(1), "removable"));
        storage.close();
        storage = StorageFactory.reopen(storage);

        storage.upsert(entry(keyAt(1), null));
        storage.close();
        storage = StorageFactory.reopen(storage);

        assertNull(storage.get(keyAt(1)));
        assertFalse(storage.allFrom(keyAt(1)).hasNext());
    }

    @Test
    void deleteOldValueFromMemory() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entry(keyAt(1), "removable"));
        storage.close();
        storage = StorageFactory.reopen(storage);

        storage.upsert(entry(keyAt(1), null));

        assertNull(storage.get(keyAt(1)));
        assertFalse(storage.allFrom(keyAt(1)).hasNext());
    }

    @Test
    void deleteFromMemory() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entry(keyAt(1), "removable"));
        storage.upsert(entry(keyAt(1), null));

        assertNull(storage.get(keyAt(1)));
        assertFalse(storage.allFrom(keyAt(1)).hasNext());
    }

    @Test
    void deleteFromDisk() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entry(keyAt(1), "removable"));
        storage.upsert(entry(keyAt(1), null));
        storage.close();
        storage = StorageFactory.reopen(storage);

        assertNull(storage.get(keyAt(1)));
        assertFalse(storage.allFrom(keyAt(1)).hasNext());
    }

    @Test
    void restoreOldValueInFile() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entry(keyAt(1), "removable"));
        storage.close();
        storage = StorageFactory.reopen(storage);

        storage.upsert(entry(keyAt(1), null));
        storage.close();
        storage = StorageFactory.reopen(storage);

        storage.upsert(entryAt(1));
        storage.close();
        storage = StorageFactory.reopen(storage);

        assertSame(storage.all(), 1);
    }

    @Test
    void restoreOldValueInMemory() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entry(keyAt(1), "removable"));
        storage.close();
        storage = StorageFactory.reopen(storage);

        storage.upsert(entry(keyAt(1), null));

        storage.upsert(entryAt(1));

        assertSame(storage.all(), 1);
    }

    @Test
    void restoreInMemory() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entry(keyAt(1), "removable"));
        storage.upsert(entry(keyAt(1), null));
        storage.upsert(entryAt(1));

        assertSame(storage.all(), 1);
    }

    @Test
    void restoreOnDisk() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entry(keyAt(1), "removable"));
        storage.upsert(entry(keyAt(1), null));
        storage.close();
        storage = StorageFactory.reopen(storage);

        storage.upsert(entryAt(1));
        storage.close();
        storage = StorageFactory.reopen(storage);

        assertSame(storage.all(), 1);
    }

    @Test
    void upsertNonExistent() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entry(keyAt(1), null));
        storage.close();
        storage = StorageFactory.reopen(storage);

        assertFalse(storage.all().hasNext());
    }

    @Test
    void emptyStringIsNotNull() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entry(keyAt(1), ""));
        storage.close();
        storage = StorageFactory.reopen(storage);

        assertSame(storage.get(keyAt(1)), entry(keyAt(1), ""));
    }

    @Test
    void rangeStressTest() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entryAt(1));
        for (int i = 2; i < 10; i++) {
            storage.upsert(entry(keyAt(i), null));
        }
        storage.upsert(entryAt(10));

        assertFalse(storage.get(keyAt(2), keyAt(10)).hasNext());
        assertSame(storage.allTo(keyAt(10)), 1);
        assertSame(storage.allFrom(keyAt(2)), 10);

        storage.close();
        storage = StorageFactory.reopen(storage);

        assertFalse(storage.get(keyAt(2), keyAt(10)).hasNext());
        assertSame(storage.allTo(keyAt(10)), 1);
        assertSame(storage.allFrom(keyAt(2)), 10);

        storage.upsert(entryAt(5));

        assertSame(storage.get(keyAt(2), keyAt(10)), 5);
        assertSame(storage.allTo(keyAt(10)), 1, 5);
        assertSame(storage.allFrom(keyAt(2)), 5, 10);

        storage.close();
        storage = StorageFactory.reopen(storage);

        assertSame(storage.get(keyAt(2), keyAt(10)), 5);
        assertSame(storage.allTo(keyAt(10)), 1, 5);
        assertSame(storage.allFrom(keyAt(2)), 5, 10);
    }

    @Test
    void checkFlow() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        NavigableSet<Entry<String>> values = new TreeSet<>(Comparator.comparing(Entry::key));
        for (int i = 1; i <= 100; i++) {
            values.add(entryAt(i));
            storage.upsert(entryAt(i));
        }

        for (int i = 75; i <= 90; i++) {
            removeValue(i, storage, values);
        }

        performChecks(storage, values);

        storage.close();
        storage = StorageFactory.reopen(storage);

        for (int i = 30; i <= 60; i++) {
            removeValue(i, storage, values);
        }

        performChecks(storage, values);

        storage.close();
        storage = StorageFactory.reopen(storage);

        for (int i = 5; i <= 15; i++) {
            removeValue(i, storage, values);
        }

        performChecks(storage, values);

        for (int i = 45; i <= 60; i++) {
            addValue(i, storage, values);
        }

        performChecks(storage, values);

        storage.close();
        storage = StorageFactory.reopen(storage);

        performChecks(storage, values);
    }

    void performChecks(Storage<String, Entry<String>> storage, NavigableSet<Entry<String>> values) throws IOException {

        assertSame(storage.all(), List.copyOf(values));
        int oneForth = values.size() / 4;
        for (int i = 1; i < 4; i++) {
            assertSame(storage.allFrom(keyAt(i * oneForth)),
                    List.copyOf(values.tailSet(entryAt(i * oneForth))));
            assertSame(storage.allTo(keyAt(i * oneForth)),
                    List.copyOf(values.headSet(entryAt(i * oneForth))));
        }
        int size = values.size();
        assertSame(storage.get(keyAt(size / 4), keyAt(size - size / 4)),
                List.copyOf(values.subSet(entryAt(size / 4), entryAt(size - size / 4))));
    }

    void removeValue(int value, Storage<String, Entry<String>> storage, NavigableSet<Entry<String>> values) {
        storage.upsert(entry(keyAt(value), null));
        values.remove(entryAt(value));
    }

    void addValue(int value, Storage<String, Entry<String>> storage, NavigableSet<Entry<String>> values) {
        storage.upsert(entryAt(value));
        values.add(entryAt(value));
    }
}
