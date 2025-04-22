package persistance;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import ru.axothy.api.Entry;
import ru.axothy.api.Storage;
import utils.BaseTest;
import utils.StorageFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PersistentTest extends BaseTest {

    @Test
    void persistent() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();
        
        storage.upsert(entryAt(1));
        storage.close();

        storage = StorageFactory.reopen(storage);
        assertSame(storage.get(keyAt(1)), entryAt(1));
    }

    @Test
    void multiLine() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();
        
        final Entry<String> entry = entry("key1\nkey2", "value1\nvalue2");
        storage.upsert(entry);
        storage.close();

        storage = StorageFactory.reopen(storage);
        assertSame(storage.get(entry.key()), entry);
    }

    @Test
    void variability() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();
        
        final Collection<Entry<String>> entries =
                List.of(
                        entry("key1", "value1"),
                        entry("key10", "value10"),
                        entry("key1000", "value1000"));
        entries.forEach(storage::upsert);
        storage.close();

        storage = StorageFactory.reopen(storage);
        for (final Entry<String> entry : entries) {
            assertSame(storage.get(entry.key()), entry);
        }
        storage.close();
    }

    @Test
    void cleanup() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();
        
        storage.upsert(entryAt(1));
        storage.close();

        cleanUpPersistentData(storage);
        storage = StorageFactory.reopen(storage);

        Assertions.assertNull(storage.get(keyAt(1)));
    }

    @Test
    void persistentPreventInMemoryStorage() throws Exception {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();
        
        int keys = 175_000;
        int entityIndex = keys / 2 - 7;

        // Fill
        List<Entry<String>> entries = entries(keys);
        for (int entry = 0; entry < keys; entry++) {
            final int e = entry;

            // Retry if autoflush is too slow
            retry(() -> storage.upsert(entries.get(e)));
        }
        storage.close();

        // Materialize to consume heap
        List<Entry<String>> tmp = new ArrayList<>(entries);

        assertValueAt(StorageFactory.reopen(storage), entityIndex);

        assertSame(
                tmp.get(entityIndex),
                entries.get(entityIndex)
        );
    }

    @Test
    void replaceWithClose() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        String key = "key";
        Entry<String> e1 = entry(key, "value1");
        Entry<String> e2 = entry(key, "value2");

        // Initial insert
        try (Storage<String, Entry<String>> storage1 = storage) {
            storage1.upsert(e1);

            assertSame(storage1.get(key), e1);
        }

        // Reopen and replace
        try (Storage<String, Entry<String>> storage2 = StorageFactory.reopen(storage)) {
            assertSame(storage2.get(key), e1);
            storage2.upsert(e2);
            assertSame(storage2.get(key), e2);
        }

        // Reopen and check
        try (Storage<String, Entry<String>> storage3 = StorageFactory.reopen(storage)) {
            assertSame(storage3.get(key), e2);
        }
    }

    @Disabled
    @Test
    void differentKeyValues() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        String key1 = "long key";
        String key2 = "short key";
        String key3 = "third key, yes yes";
        Entry<String> e1 = entry(key1, "value for long key");
        Entry<String> e2 = entry(key2, "value for short key");
        Entry<String> e3 = entry(key3, "value for short key 3, yes yes");

        // Initial insert
        try (Storage<String, Entry<String>> storage1 = storage) {
            storage1.upsert(e1);
            storage1.upsert(e2);
            storage1.upsert(e3);
            assertSame(storage1.get(key1), e1);
            assertSame(storage1.get(key2), e2);
            assertSame(storage1.get(key3), e3);
        }

        // Reopen and replace
        try (Storage<String, Entry<String>> storage2 = StorageFactory.reopen(storage)) {
            assertSame(storage2.get(key1), e1);
            assertSame(storage2.get(key2), e2);
            assertSame(storage2.get(key3), e3);
            storage2.upsert(e2);
            assertSame(storage2.get(key1), e1);
            assertSame(storage2.get(key2), e2);
            assertSame(storage2.get(key3), e3);
        }

        // Reopen and check
        try (Storage<String, Entry<String>> storage3 = StorageFactory.reopen(storage)) {
            assertNull(storage3.get(key1));
            assertSame(storage3.get(key2), e2);
            assertNull(storage3.get(key3));
        }
    }

    @Test
    @Timeout(value = 30)
    void toManyFiles() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();
        
        for (int i = 0; i < 30000; i++) {
            storage.close();
            storage = StorageFactory.reopen(storage);
        }
    }
}
