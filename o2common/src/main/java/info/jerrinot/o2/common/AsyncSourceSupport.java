package info.jerrinot.o2.common;

import com.hazelcast.internal.util.concurrent.ManyToOneConcurrentArrayQueue;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.WatermarkGenerationParams;
import com.hazelcast.jet.function.DistributedObjLongBiFunction;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.jet.pipeline.StreamSource;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Queue;
import java.util.function.Function;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.dontParallelize;
import static info.jerrinot.o2.impl.CloningSupplier.cloneAndSupply;

@FunctionalInterface
public interface AsyncSourceSupport<T> extends Serializable {

    void start(Processor.Context context, Function<T, Boolean> consumer);

    final class MySource<T> extends AbstractProcessor implements Serializable {
        private static final int QUEUE_CAPACITY = 1024;
        private final AsyncSourceSupport<T> asyncSourceSupport;
        private final WatermarkGenerationParams<T> watermarkGenerationParams;
        private transient Queue<T> pendingObjects = new ManyToOneConcurrentArrayQueue<>(QUEUE_CAPACITY);
        private transient Object pendingItem;

        private MySource(AsyncSourceSupport asyncSourceSupport, WatermarkGenerationParams<T> watermarkGenerationParams) {
            this.asyncSourceSupport = asyncSourceSupport;
            this.watermarkGenerationParams = watermarkGenerationParams;
        }

        private void readObject(ObjectInputStream in) throws IOException,
                ClassNotFoundException {
            in.defaultReadObject();
            pendingObjects = new ManyToOneConcurrentArrayQueue<>(QUEUE_CAPACITY);
        }

        @Override
        protected void init(Context context) {
            asyncSourceSupport.start(context, e -> pendingObjects.offer(e));
        }

        @Override
        public boolean isCooperative() {
            return true;
        }

        @Override
        public boolean complete() {
            if (pendingItem == null) {
                pendingItem = pollAndWrap();
            }
            for (;;) {
                if (pendingItem == null) {
                    return false;
                }

                if (!tryEmit(pendingItem)) {
                    return false;
                }
                pendingItem = pollAndWrap();
            }
        }

        private Object pollAndWrap() {
            T polledItem = pendingObjects.poll();
            if (polledItem == null) {
                return null;
            }
            DistributedToLongFunction<T> timestampFn = watermarkGenerationParams.timestampFn();
            long ts = timestampFn.applyAsLong(polledItem);
            DistributedObjLongBiFunction<T, ?> wrapFn = watermarkGenerationParams.wrapFn();
            return wrapFn.apply(polledItem, ts);
        }
    }

    default StreamSource<T> asSource() {
        return new StreamSourceTransform<>(toString(), w -> dontParallelize(
                cloneAndSupply(new MySource<T>(this, w))));
    }
}
