package info.jerrinot.jmssink.api;

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedIntFunction;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.dontParallelize;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeBufferedP;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

final class Sinks {

    static <E> ProcessorMetaSupplier writeSupport(SimpleSink<E> sinkSupport) {
        DistributedIntFunction<Object> createSenderFb;
        DistributedConsumer<Object> disposeBufferFn;
        if (sinkSupport instanceof LifecycleAware) {
            createSenderFb = ignored -> {
                    uncheckRun(() -> ((LifecycleAware)sinkSupport).start());
                    return null;
            };
            disposeBufferFn = ignored -> uncheckRun(() -> ((LifecycleAware)sinkSupport).stop());
        } else {
            createSenderFb = ignored -> null;
            disposeBufferFn = ignored -> {};
        }
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