package info.jerrinot.jmssink.api;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Sink;

import java.io.Serializable;

import static com.hazelcast.jet.Sinks.fromProcessor;
import static info.jerrinot.jmssink.api.Sinks.writeSupport;

/**
 * Convenient class for creating custom sinks.
 *
 * @param <E>
 */
public abstract class SinkSupport<E> implements Serializable {
    public abstract void doInvoke(E o) throws Exception;

    public void start() throws Exception {
        //intentionally no-op
    }

    public void stop() throws Exception {
        //intentionally no-op
    }

    public final void invoke(E o) {
        try {
            doInvoke(o);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new JetException("Error while writing to sink '" + toString() + "'", e);
        }
    }

    public static <E> Sink<E> asSink(SinkSupport<E> sinkSupport) {
        return fromProcessor(sinkSupport.toString(), writeSupport(sinkSupport));
    }
}
