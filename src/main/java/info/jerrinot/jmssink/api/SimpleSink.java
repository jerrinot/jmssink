package info.jerrinot.jmssink.api;

import java.io.Serializable;

public interface SimpleSink<E> extends Serializable {
    void invoke(E o);
}
