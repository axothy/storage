package persistance;

import org.junit.jupiter.api.Test;
import ru.axothy.api.Entry;
import ru.axothy.api.Storage;
import utils.BaseTest;
import ru.axothy.storage.StorageFactory;

import java.io.IOException;

public class UpsertRemoveTest extends BaseTest {

    @Test
    void persistentRemoveTest() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entryAt(1));
        storage.close();

        storage = StorageFactory.reopen(storage);
        storage.upsert(entry(keyAt(1), null));
        assertSame(storage.get(keyAt(1)), null);
    }

    @Test
    void persistentRemoveTestRange() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entryAt(1));
        storage.upsert(entryAt(2));
        storage.upsert(entry(keyAt(2), null));
        storage.close();

        storage = StorageFactory.reopen(storage);

        assertSame(storage.all(), entryAt(1));
    }

    @Test
    void persistentRemoveTestRangeInMemory() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entryAt(1));
        storage.upsert(entryAt(2));
        storage.close();

        storage = StorageFactory.reopen(storage);
        storage.upsert(entry(keyAt(2), null));

        assertSame(storage.all(), entryAt(1));
    }

    @Test
    void persistentGetAfterRemoveTestRange() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entryAt(1));
        storage.upsert(entryAt(2));
        storage.upsert(entry(keyAt(2), null));
        storage.close();

        storage = StorageFactory.reopen(storage);
        storage.upsert(entryAt(2));

        assertSame(storage.all(), entryAt(1), entryAt(2));
    }

    @Test
    void persistentGetAfterRemoveTestRangeFromMemory() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entryAt(1));
        storage.upsert(entryAt(2));
        storage.close();

        storage = StorageFactory.reopen(storage);
        storage.upsert(entry(keyAt(2), null));
        storage.upsert(entryAt(2));

        assertSame(storage.all(), entryAt(1), entryAt(2));
    }

    @Test
    void manyRemoveRecords() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        for (int i = 0; i < 6; i++) {
            storage.upsert(entryAt(i));
        }
        storage.close();

        storage = StorageFactory.reopen(storage);
        storage.upsert(entry(keyAt(1), null));
        storage.upsert(entry(keyAt(2), null));
        storage.upsert(entry(keyAt(4), null));
        storage.upsert(entry(keyAt(5), null));
        storage.close();

        storage = StorageFactory.reopen(storage);
        storage.upsert(entry(keyAt(3), null));
        storage.upsert(entryAt(4));
        storage.upsert(entry(keyAt(5), "new value"));
        storage.close();

        storage = StorageFactory.reopen(storage);
        storage.upsert(entryAt(2));
        storage.upsert(entryAt(1));

        assertSame(
                storage.all(),
                entryAt(0),
                entryAt(1),
                entryAt(2),
                entryAt(4),
                entry(keyAt(5), "new value")
        );
    }

}
