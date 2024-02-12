package ly.persona.academic.data;

import java.io.Closeable;

public interface DataWriter<T> extends Closeable {
    void write(T data);

    @Override
    default void close() {
    }
}
