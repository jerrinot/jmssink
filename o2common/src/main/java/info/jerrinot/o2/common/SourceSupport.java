package info.jerrinot.o2.common;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.WatermarkGenerationParams;
import com.hazelcast.jet.function.DistributedObjLongBiFunction;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;

import java.io.Serializable;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.dontParallelize;
import static info.jerrinot.o2.impl.CloningSupplier.cloneAndSupply;

public abstract class SourceSupport<T> implements Serializable {

    public abstract T poll() throws Exception;

    public void init(Processor.Context context) throws Exception {
        //intentionally no-op
    }


    private final static class MySource extends AbstractProcessor implements Serializable {
        private final SourceSupport sourceSupport;
        private Object pendingItem;

        public MySource(SourceSupport<?> sourceSupport) {
            this.sourceSupport = sourceSupport;
        }

        @Override
        protected void init(Context context) throws Exception {
            sourceSupport.init(context);
        }

        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean complete() {
            if (pendingItem != null) {
                if (!tryEmit(pendingItem)) {
                    return false;
                }
            }
            for (;;) {
                try {
                    pendingItem = sourceSupport.poll();
                }  catch (Exception e) {
                    throw ExceptionUtil.sneakyThrow(e);
                }
                if (pendingItem == null) {
                    return false;
                }
                if (!tryEmit(pendingItem)) {
                    return false;
                }
            }
        }
    }

    public final StreamSource<T> asSource() {
        StreamSourceTransform<T> src = new StreamSourceTransform<>(this.toString(), w -> dontParallelize(
                cloneAndSupply(new MySource(this))));
//        return Sources.streamFromProcessor(this.toString(), w -> dontParallelize(
//                cloneAndSupply(new MySource(this, w)))
        return src;
    }
}
