import ru.axothy.api.Entry;
import ru.axothy.api.Storage;
import ru.axothy.config.Config;

import java.io.IOException;

public interface StorageFactory<D, E extends Entry<D>> {

    default Storage<D, E> createStorage() throws IOException {
        throw new UnsupportedOperationException("Need to override one of createDao methods");
    }

    default Storage<D, E> createStorage(Config config) throws IOException {
        return createStorage();
    }

    String toString(D data);

    D fromString(String data);

    E fromBaseEntry(Entry<D> baseEntry);

    static Config extractConfig(Storage<String, Entry<String>> dao) {
        return ((StringStorage<?,?>)dao).getConfig();
    }

    static Storage<String, Entry<String>> reopen(Storage<String, Entry<String>> dao) throws IOException {
        return ((StringStorage<?,?>)dao).reopen();
    }

    default Storage<String, Entry<String>> createStringStorage(Config config) throws IOException {
        return new StringStorage<>(this, config);
    }
}