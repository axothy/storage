package persistance;

import org.junit.jupiter.api.Test;
import ru.axothy.api.Entry;
import ru.axothy.api.Storage;
import utils.BaseTest;
import utils.StorageFactory;

import java.io.IOException;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PersistentRangeTest extends BaseTest {
    public static final int[] NOTHING = new int[0];
    private static final int[] DATASET = new int[]{7, 97, 101};

    private void sliceAndDice(Storage<String, Entry<String>> storage) throws IOException {
        // Full
        assertSame(storage.all(), DATASET);

        // From
        assertSame(storage.allFrom(keyAt(6)), DATASET);
        assertSame(storage.allFrom(keyAt(7)), DATASET);
        assertSame(storage.allFrom(keyAt(8)), 97, 101);
        assertSame(storage.allFrom(keyAt(96)), 97, 101);
        assertSame(storage.allFrom(keyAt(97)), 97, 101);
        assertSame(storage.allFrom(keyAt(98)), 101);
        assertSame(storage.allFrom(keyAt(100)), 101);
        assertSame(storage.allFrom(keyAt(101)), 101);
        assertSame(storage.allFrom(keyAt(102)), NOTHING);

        // Right
        assertSame(storage.allTo(keyAt(102)), DATASET);
        assertSame(storage.allTo(keyAt(101)), 7, 97);
        assertSame(storage.allTo(keyAt(100)), 7, 97);
        assertSame(storage.allTo(keyAt(98)), 7, 97);
        assertSame(storage.allTo(keyAt(97)), 7);
        assertSame(storage.allTo(keyAt(96)), 7);
        assertSame(storage.allTo(keyAt(8)), 7);
        assertSame(storage.allTo(keyAt(7)), NOTHING);
        assertSame(storage.allTo(keyAt(6)), NOTHING);

        // Between

        assertSame(storage.get(keyAt(6), keyAt(102)), DATASET);
        assertSame(storage.get(keyAt(7), keyAt(102)), DATASET);

        assertSame(storage.get(keyAt(6), keyAt(101)), 7, 97);
        assertSame(storage.get(keyAt(7), keyAt(101)), 7, 97);
        assertSame(storage.get(keyAt(7), keyAt(98)), 7, 97);

        assertSame(storage.get(keyAt(7), keyAt(97)), 7);
        assertSame(storage.get(keyAt(6), keyAt(97)), 7);
        assertSame(storage.get(keyAt(6), keyAt(96)), 7);
        assertSame(storage.get(keyAt(6), keyAt(8)), 7);
        assertSame(storage.get(keyAt(7), keyAt(7)), NOTHING);

        assertSame(storage.get(keyAt(97), keyAt(102)), 97, 101);
        assertSame(storage.get(keyAt(96), keyAt(102)), 97, 101);
        assertSame(storage.get(keyAt(98), keyAt(102)), 101);
        assertSame(storage.get(keyAt(98), keyAt(101)), NOTHING);
        assertSame(storage.get(keyAt(98), keyAt(100)), NOTHING);
        assertSame(storage.get(keyAt(102), keyAt(1000)), NOTHING);
        assertSame(storage.get(keyAt(0), keyAt(7)), NOTHING);
        assertSame(storage.get(keyAt(0), keyAt(6)), NOTHING);
    }

    @Test
    void justMemory() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entryAt(101));
        storage.upsert(entryAt(97));
        storage.upsert(entryAt(7));

        sliceAndDice(storage);
    }

    @Test
    void justDisk() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entryAt(97));
        storage.upsert(entryAt(7));
        storage.upsert(entryAt(101));
        storage.close();
        storage = StorageFactory.reopen(storage);

        sliceAndDice(storage);
    }

    @Test
    void justDisk2() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entryAt(101));
        storage.upsert(entryAt(97));
        storage.close();
        storage = StorageFactory.reopen(storage);

        storage.upsert(entryAt(7));
        storage.close();
        storage = StorageFactory.reopen(storage);

        sliceAndDice(storage);
    }

    @Test
    void mixedMemoryDisk() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entryAt(7));
        storage.upsert(entryAt(97));
        storage.close();
        storage = StorageFactory.reopen(storage);

        storage.upsert(entryAt(101));

        sliceAndDice(storage);
    }

    @Test
    void mixedMemoryDisk2() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entryAt(7));
        storage.close();
        storage = StorageFactory.reopen(storage);

        storage.upsert(entryAt(97));
        storage.close();
        storage = StorageFactory.reopen(storage);

        storage.upsert(entryAt(101));

        sliceAndDice(storage);
    }

    @Test
    void replaceWithMemory() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entryAt(7));
        storage.upsert(entry(keyAt(97), "old97"));
        storage.upsert(entryAt(101));
        storage.close();
        storage = StorageFactory.reopen(storage);

        storage.upsert(entryAt(97));

        sliceAndDice(storage);
    }

    @Test
    void replaceOnDisk() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entryAt(7));
        storage.upsert(entry(keyAt(97), "old97"));
        storage.upsert(entryAt(101));
        storage.close();
        storage = StorageFactory.reopen(storage);

        storage.upsert(entryAt(97));
        storage.close();
        storage = StorageFactory.reopen(storage);

        sliceAndDice(storage);
    }

    @Test
    void fresh() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entry(keyAt(7), "old7"));
        storage.upsert(entryAt(97));
        storage.upsert(entry(keyAt(101), "old101"));
        storage.close();
        storage = StorageFactory.reopen(storage);

        storage.upsert(entryAt(7));
        storage.close();
        storage = StorageFactory.reopen(storage);

        storage.upsert(entryAt(101));

        sliceAndDice(storage);
    }

    @Test
    void concat() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        final int flushes = 10;
        final int entries = 1000;
        final int step = 7;

        int i = 0;
        for (int flush = 0; flush < flushes; flush++) {
            for (int entry = 0; entry < entries; entry++) {
                storage.upsert(entryAt(i));
                i += step;
            }
            storage.close();
            storage = StorageFactory.reopen(storage);
        }

        final Iterator<Entry<String>> all = storage.all();
        int expected = 0;
        while (i > 0) {
            assertTrue(all.hasNext());
            assertSame(all.next(), entryAt(expected));
            expected += step;
            i -= step;
        }
    }

    @Test
    void interleave() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        final int flushes = 10;
        final int entries = 1000;

        for (int flush = 0; flush < flushes; flush++) {
            int i = flush;
            for (int entry = 0; entry < entries; entry++) {
                storage.upsert(entryAt(i));
                i += flushes;
            }
            storage.close();
            storage = StorageFactory.reopen(storage);
        }

        final Iterator<Entry<String>> all = storage.all();
        final int limit = flushes * entries;
        int expected = 0;
        while (expected < limit) {
            assertTrue(all.hasNext());
            assertSame(all.next(), entryAt(expected));
            expected++;
        }
    }

    @Test
    void overwrite() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        final int flushes = 10;
        final int entries = 1000;

        for (int flush = 0; flush < flushes; flush++) {
            for (int entry = 0; entry < entries; entry++) {
                storage.upsert(entryAt(entry));
            }
            storage.close();
            storage = StorageFactory.reopen(storage);
        }

        final Iterator<Entry<String>> all = storage.all();
        for (int entry = 0; entry < entries; entry++) {
            assertTrue(all.hasNext());
            assertSame(all.next(), entryAt(entry));
        }
    }

    @Test
    void memoryCemetery() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        final int entries = 100_000;

        for (int entry = 0; entry < entries; entry++) {
            storage.upsert(entry(keyAt(entry), null));
        }

        storage.close();
        storage = StorageFactory.reopen(storage);

        assertFalse(storage.all().hasNext());
    }

    @Test
    void diskCemetery() throws Exception {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        final int entries = 100_000;

        for (int entry = 0; entry < entries; entry++) {
            storage.upsert(entry(keyAt(entry), null));

            // Back off after 1K upserts to be able to flush
            if (entry % 1000 == 0) {
                Thread.sleep(1);
            }
        }

        assertFalse(storage.all().hasNext());
    }
}
