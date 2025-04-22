package persistance;

import org.junit.jupiter.api.Test;
import ru.axothy.api.Entry;
import ru.axothy.api.Storage;
import utils.BaseTest;
import utils.StorageFactory;

import java.io.IOException;
import java.nio.channels.IllegalBlockingModeException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class PersistentConcurrentTest extends BaseTest {
    @Test
    void testConcurrentRW_2_500_2() throws Exception {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        int count = 2_500;
        List<Entry<String>> entries = entries("k", "v", count);
        runInParallel(4, count, value -> {
            storage.upsert(entries.get(value));
        }).close();
        storage.close();

        Storage<String, Entry<String>> storage2 = StorageFactory.reopen(storage);
        runInParallel(4, count, value -> {
            assertSame(storage2.get(entries.get(value).key()), entries.get(value));
        }).close();
    }

    @Test
    void testConcurrentRW_100_000_compact() throws Exception {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();

        int count = 100_000;

        List<Entry<String>> entries = entries("k", "v", count);
        long timeoutNanosWarmup = TimeUnit.MILLISECONDS.toNanos(1000);
        runInParallel(4, count, value -> {
            retry(timeoutNanosWarmup, () -> storage.upsert(entries.get(value)));
            retry(timeoutNanosWarmup, () -> storage.upsert(entry(keyAt(value), null)));
            retry(timeoutNanosWarmup, () -> storage.upsert(entries.get(value)));
        }, () -> {
            for (int i = 0; i < 100; i++) {
                try {
                    runAndMeasure(timeoutNanosWarmup, storage::compact);
                    runAndMeasure(timeoutNanosWarmup, storage::flush);

                    Thread.sleep(30);
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).close();

        // 200ms should be enough considering GC
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(200);

        runInParallel(4, count, value -> {
            retry(timeoutNanos, () -> storage.upsert(entries.get(value)));
            retry(timeoutNanos, () -> storage.upsert(entry(keyAt(value), null)));
            retry(timeoutNanos, () -> storage.upsert(entries.get(value)));
        }, () -> {
            for (int i = 0; i < 100; i++) {
                try {
                    runAndMeasure(timeoutNanos, storage::compact);
                    runAndMeasure(timeoutNanos, storage::flush);

                    Thread.sleep(30);
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).close();
        storage.close();

        Storage<String, Entry<String>> storage2 = StorageFactory.reopen(storage);
        runInParallel(
                4,
                count,
                value -> assertSame(storage2.get(entries.get(value).key()), entries.get(value))).close();
    }

    private static <E extends Exception> void runAndMeasure(
            long timeoutNanos,
            ErrorableTask<E> runnable) throws E {
        long start = System.nanoTime();
        runnable.run();
        long elapsedNanos = System.nanoTime() - start;

        // Check timeout
        if (elapsedNanos > timeoutNanos) {
            throw new IllegalBlockingModeException();
        }
    }
}