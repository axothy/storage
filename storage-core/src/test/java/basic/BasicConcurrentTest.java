package basic;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import ru.axothy.api.Entry;
import ru.axothy.api.Storage;
import utils.BaseTest;

import java.util.List;

public class BasicConcurrentTest extends BaseTest {

    @Test
    void test_10_000() throws Exception {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        int count = 10_000;
        List<Entry<String>> entries = entries("k", "v", count);
        runInParallel(4, count, value -> storage.upsert(entries.get(value))).close();
        assertSame(storage.all(), entries);
    }

    @Test
    @Timeout(15)
    void testConcurrentRW_2_500() throws Exception {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        int count = 2_500;
        List<Entry<String>> entries = entries("k", "v", count);
        runInParallel(4, count, value -> {
            storage.upsert(entries.get(value));
            assertContains(storage.all(), entries.get(value));
        }).close();

        assertSame(storage.all(), entries);
    }

    @Test
    void testConcurrentRead_8_000() throws Exception {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        int count = 8_000;
        List<Entry<String>> entries = entries("k", "v", count);
        for (Entry<String> entry : entries) {
            storage.upsert(entry);
        }
        runInParallel(4, count, value -> assertContains(storage.all(), entries.get(value))).close();

        assertSame(storage.all(), entries);
    }

}
