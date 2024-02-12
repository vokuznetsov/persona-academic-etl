package ly.persona.academic.data;

import java.io.Closeable;

public interface DataReader<T> extends Closeable {
    T read();

    @Override
    default void close() {
    }
}
