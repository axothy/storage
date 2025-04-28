package basic;

import org.junit.jupiter.api.Test;
import ru.axothy.api.Entry;
import ru.axothy.api.Storage;
import ru.axothy.storage.BaseEntry;
import utils.BaseTest;
import ru.axothy.storage.StorageFactory;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MusicTest extends BaseTest {
    private static final char DELIMITER = '\0';
    private static final char DELIMITER_FOR_SUFFIX = DELIMITER + 1;

    @Test
    void database() throws Exception {
        Storage<String, Entry<String>> storage = BaseTest.getStringEntryStorage();
        
        // Fill the music database
        storage.upsert(record(trackFrom("Ar1", "Al11", "T111"), 15));
        storage.upsert(record(trackFrom("Ar1", "Al11", "T112"), 24));
        storage.upsert(record(trackFrom("Ar1", "Al12", "T111"), 33));
        storage.upsert(record(trackFrom("Ar1", "Al12", "T1111"), 49));
        storage.upsert(record(trackFrom("Ar1", "Al12", "T112"), 50));
        storage.upsert(record(trackFrom("Ar2", "Al21", "T211"), 62));
        storage.upsert(record(trackFrom("Ar2", "Al21", "T212"), 78));

        // Re-open the music database
        storage.close();
        storage = StorageFactory.reopen(storage);

        // Artists
        assertRangeSize(storage, artistFrom("Ar1"), 5);
        assertRangeSize(storage, artistFrom("Ar2"), 2);

        // Albums
        assertRangeSize(storage, albumFrom("Ar1", "Al11"), 2);
        assertRangeSize(storage, albumFrom("Ar1", "Al12"), 3);
        assertRangeSize(storage, albumFrom("Ar2", "Al21"), 2);
    }

    private static String artistFrom(String artist) {
        assertEquals(-1, artist.indexOf(DELIMITER));
        assertEquals(-1, artist.indexOf(DELIMITER_FOR_SUFFIX));

        return artist;
    }

    private static String albumFrom(String artist, String album) {
        assertEquals(-1, artist.indexOf(DELIMITER));
        assertEquals(-1, artist.indexOf(DELIMITER_FOR_SUFFIX));
        assertEquals(-1, album.indexOf(DELIMITER));
        assertEquals(-1, album.indexOf(DELIMITER_FOR_SUFFIX));

        return artist + DELIMITER + album;
    }

    private static String trackFrom(
            String artist,
            String album,
            String track
    ) {
        assertEquals(-1, artist.indexOf(DELIMITER));
        assertEquals(-1, artist.indexOf(DELIMITER_FOR_SUFFIX));
        assertEquals(-1, album.indexOf(DELIMITER));
        assertEquals(-1, album.indexOf(DELIMITER_FOR_SUFFIX));
        assertEquals(-1, track.indexOf(DELIMITER));
        assertEquals(-1, track.indexOf(DELIMITER_FOR_SUFFIX));

        return artist + DELIMITER + album + DELIMITER + track;
    }

    private static Entry<String> record(String track, int duration) {
        return new BaseEntry<>(
                track,
                duration(duration)
        );
    }

    private static String duration(int seconds) {
        return Integer.toString(seconds);
    }

    private void assertRangeSize(
            Storage<String, Entry<String>> storage,
            String suffix,
            int count) throws Exception {
        Iterator<Entry<String>> range = storage.get(
                suffix + DELIMITER,
                suffix + DELIMITER_FOR_SUFFIX
        );

        int size = 0;
        while (range.hasNext()) {
            size++;
            range.next();
        }

        assertEquals(count, size);
    }
}
