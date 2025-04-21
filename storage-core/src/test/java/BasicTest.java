import org.junit.jupiter.api.Test;
import ru.axothy.api.Entry;
import ru.axothy.api.Storage;
import ru.axothy.config.Config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class BasicTest extends BaseTest {

    @Test
    void testEmpty() throws IOException {
        Path tmp = Files.createTempDirectory("dao");
        long flushThreshold = 1 << 20; // 1 MB

        Storage<String, Entry<String>> storage = new MemorySegmentStorageFactory().createStringStorage(new Config(tmp, flushThreshold, 0.2));
        assertEmpty(storage.all());
    }

    @Test
    void testSingle() throws IOException {
        Path tmp = Files.createTempDirectory("dao");
        long flushThreshold = 1 << 20; // 1 MB

        Storage<String, Entry<String>> storage = new MemorySegmentStorageFactory().createStringStorage(new Config(tmp, flushThreshold, 0.2));

        storage.upsert(entry("a", "b"));
        assertSame(
                storage.all(),
                entry("a", "b")
        );
    }
}
