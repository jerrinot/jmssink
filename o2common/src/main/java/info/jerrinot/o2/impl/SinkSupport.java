package info.jerrinot.o2.impl;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Sink;
import com.hazelcast.jet.Sinks;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedIntFunction;
import com.hazelcast.jet.impl.util.Util;

import java.io.Serializable;

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

    public final Sink<E> asSink() {
        return Sinks.fromProcessor(this.toString(), writeSupport(this));
    }

    private ProcessorMetaSupplier writeSupport(SinkSupport<E> sinkSupport) {
        DistributedIntFunction<Object> createSenderFb;
        DistributedConsumer<Object> disposeBufferFn;
        createSenderFb = ignored -> {
            Util.uncheckRun(sinkSupport::start);
            return null;
        };
        disposeBufferFn = ignored -> Util.uncheckRun(sinkSupport::stop);
        DistributedBiConsumer<Object, E> addToBufferFn = (ignored, e) -> sinkSupport.invoke(e);
        return ProcessorMetaSupplier.dontParallelize(
                SinkProcessors.writeBufferedP(
                        createSenderFb,
                        addToBufferFn,
                        i -> {
                        },
                        disposeBufferFn
                )
        );
    }
}
