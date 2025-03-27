import org.junit.jupiter.api.Test;
import ru.axothy.api.Dao;
import ru.axothy.api.Entry;
import ru.axothy.config.Config;
import ru.axothy.storage.LSMDao;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;

public class BasicTest extends BaseTest {

    @Test
    void testEmpty() throws IOException {
        Dao<MemorySegment, Entry<MemorySegment>> dao = getDao();
        assertEmpty(dao.all());
    }

    private static Dao<MemorySegment, Entry<MemorySegment>> getDao() throws IOException {
        Path tmp = Files.createTempDirectory("dao");
        long flushThreshold = 1 << 20; // 1 MB

        return new LSMDao(new Config(tmp, flushThreshold, 0.2));
    }
}
