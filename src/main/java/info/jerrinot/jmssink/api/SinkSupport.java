package info.jerrinot.jmssink.api;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Sink;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedIntFunction;

import java.io.Serializable;

import static com.hazelcast.jet.Sinks.fromProcessor;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.dontParallelize;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeBufferedP;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

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

    public Sink<E> asSink() {
        return fromProcessor(this.toString(), writeSupport(this));
    }

    private ProcessorMetaSupplier writeSupport(SinkSupport<E> sinkSupport) {
        DistributedIntFunction<Object> createSenderFb;
        DistributedConsumer<Object> disposeBufferFn;
        createSenderFb = ignored -> {
            uncheckRun(sinkSupport::start);
            return null;
        };
        disposeBufferFn = ignored -> uncheckRun(sinkSupport::stop);
        DistributedBiConsumer<Object, E> addToBufferFn = (ignored, e) -> sinkSupport.invoke(e);
        return dontParallelize(
                writeBufferedP(
                        createSenderFb,
                        addToBufferFn,
                        i -> {
                        },
                        disposeBufferFn
                )
        );
    }
}
