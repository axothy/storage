package utils;

import ru.axothy.api.Entry;
import ru.axothy.api.Storage;
import ru.axothy.config.Config;
import ru.axothy.storage.BaseEntry;

import java.io.IOException;
import java.util.Iterator;

public class StringStorage<D, E extends Entry<D>> implements Storage<String, Entry<String>> {

    private Storage<D, E> delegate;

    private final StorageFactory<D, E> factory;

    private final Config config;

    public StringStorage(StorageFactory<D, E> factory, Config config) throws IOException {
        this.factory = factory;
        this.config = config;
        this.delegate = factory.createStorage(config);
    }

    public Storage<String, Entry<String>> reopen() throws IOException {
        delegate = factory.createStorage(config);
        return this;
    }

    @Override
    public Entry<String> get(String key) {
        E result = delegate.get(factory.fromString(key));
        if (result == null) {
            return null;
        }
        return new BaseEntry<>(
                factory.toString(result.key()),
                factory.toString(result.value())
        );
    }

    @Override
    public Iterator<Entry<String>> get(String from, String to) {
        Iterator<E> iterator = delegate.get(
                factory.fromString(from),
                factory.fromString(to)
        );
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Entry<String> next() {
                E next = iterator.next();
                String key = factory.toString(next.key());
                String value = factory.toString(next.value());
                return new BaseEntry<>(key, value);
            }
        };
    }

    @Override
    public void upsert(Entry<String> entry) {
        BaseEntry<D> e = new BaseEntry<>(
                factory.fromString(entry.key()),
                factory.fromString(entry.value())
        );
        delegate.upsert(factory.fromBaseEntry(e));
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }

    @Override
    public void compact() throws IOException {
        delegate.compact();
    }

    @Override
    public void close() throws IOException {
        if (delegate != null) {
            delegate.close();
            delegate = null;
        }
    }

    public Config getConfig() {
        return config;
    }

}
