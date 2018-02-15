package info.jerrinot.o2.common;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedIntFunction;
import com.hazelcast.jet.impl.pipeline.JetEvent;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;

import java.io.Serializable;

/**
 * Convenient class for creating custom sinks.
 *
 * @param <E>
 */
@FunctionalInterface
public interface SinkSupport<E> extends Serializable {
    void doInvoke(E o) throws Exception;

    default void start() throws Exception {
        //intentionally no-op
    }

    default void stop() throws Exception {
        //intentionally no-op
    }

    default void invoke(E o) {
        try {
            if (o instanceof JetEvent) {
                o = (E) ((JetEvent) o).payload();
            } else {
                System.out.println("Not Jet Event");
            }
            doInvoke(o);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new JetException("Error while writing to sink '" + toString() + "'", e);
        }
    }

    default Sink<E> asSink() {
        return Sinks.fromProcessor(this.toString(), writeSupport(this));
    }

    default ProcessorMetaSupplier writeSupport(SinkSupport<E> sinkSupport) {
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
