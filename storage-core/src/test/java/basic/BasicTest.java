package basic;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;
import ru.axothy.api.Entry;
import ru.axothy.api.Storage;
import utils.BaseTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BasicTest extends BaseTest {

    @Test
    void testEmpty() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        assertEmpty(storage.all());
    }

    @Test
    void testSingle() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entry("a", "b"));
        assertSame(
                storage.all(),
                entry("a", "b")
        );
    }


    @Test
    void testOrder() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entry("b", "b"));
        storage.upsert(entry("aa", "aa"));
        storage.upsert(entry("", ""));

        assertSame(
                storage.all(),

                entry("", ""),
                entry("aa", "aa"),
                entry("b", "b")
        );
    }

    @Test
    void testOrder2() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entry("aa", "aa"));
        storage.upsert(entry("b", "b"));
        storage.upsert(entry("", ""));

        assertSame(
                storage.all(),

                entry("", ""),
                entry("aa", "aa"),
                entry("b", "b")
        );
    }

    @Test
    void testTree() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entry("e", "f"));
        storage.upsert(entry("c", "d"));
        storage.upsert(entry("a", "b"));

        assertSame(
                storage.all(),

                entry("a", "b"),
                entry("c", "d"),
                entry("e", "f")
        );
    }

    @Test
    void testManyIterators() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        List<Entry<String>> entries = new ArrayList<>(entries(10_000));
        for (Entry<String> entry : entries) {
            storage.upsert(entry);
        }

        try {
            List<Iterator<Entry<String>>> iterators = new ArrayList<>();
            for (int i = 0; i < 10_000; i++) {
                iterators.add(storage.all());
            }
            // just utilize the collection
            Assertions.assertEquals(10_000, iterators.size());
        } catch (OutOfMemoryError error) {
            throw new AssertionFailedError("Too much data in memory: use some lazy ways", error);
        }
    }

    @Test
    void testFindValueInTheMiddle() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entry("e", "f"));
        storage.upsert(entry("c", "d"));
        storage.upsert(entry("a", "b"));

        assertSame(storage.get("c"), entry("c", "d"));
    }

    @Test
    void testFindRangeInTheMiddle() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entry("e", "f"));
        storage.upsert(entry("c", "d"));
        storage.upsert(entry("a", "b"));

        assertSame(storage.get("c", "e"), entry("c", "d"));
    }

    @Test
    void testFindFullRange() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entry("e", "f"));
        storage.upsert(entry("c", "d"));
        storage.upsert(entry("a", "b"));


        assertSame(
                storage.get("a", "z"),

                entry("a", "b"),
                entry("c", "d"),
                entry("e", "f")
        );
    }

    @Test
    void testAllTo() throws IOException {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        storage.upsert(entry("e", "f"));
        storage.upsert(entry("c", "d"));
        storage.upsert(entry("a", "b"));

        assertSame(
                storage.allTo("e"),

                entry("a", "b"),
                entry("c", "d")
        );
    }

    @Test
    void testHugeData() throws Exception {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        final int entries = 100_000;

        for (int entry = 0; entry < entries; entry++) {
            final int e = entry;

            // Retry if autoflush is too slow
            BaseTest.retry(() -> storage.upsert(entry(keyAt(e), valueAt(e))));
        }

        for (int entry = 0; entry < entries; entry++) {
            assertSame(storage.get(keyAt(entry)), entryAt(entry));
        }
    }
}
