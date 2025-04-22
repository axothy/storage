package utils;

import ru.axothy.api.Entry;
import ru.axothy.api.Storage;
import ru.axothy.config.Config;

import java.io.IOException;

public interface StorageFactory<D, E extends Entry<D>> {

    default Storage<D, E> createStorage() throws IOException {
        throw new UnsupportedOperationException("Need to override one of createStorage methods");
    }

    default Storage<D, E> createStorage(Config config) throws IOException {
        return createStorage();
    }

    String toString(D data);

    D fromString(String data);

    E fromBaseEntry(Entry<D> baseEntry);

    static Config extractConfig(Storage<String, Entry<String>> storage) {
        return ((StringStorage<?,?>)storage).getConfig();
    }

    static Storage<String, Entry<String>> reopen(Storage<String, Entry<String>> storage) throws IOException {
        return ((StringStorage<?,?>)storage).reopen();
    }

    default Storage<String, Entry<String>> createStringStorage(Config config) throws IOException {
        return new StringStorage<>(this, config);
    }
}