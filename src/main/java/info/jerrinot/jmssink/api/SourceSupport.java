package info.jerrinot.jmssink.api;

import com.hazelcast.jet.Source;
import com.hazelcast.jet.Sources;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.util.ExceptionUtil;

import java.io.Serializable;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.dontParallelize;
import static info.jerrinot.jmssink.impl.CloningSupplier.cloneAndSupply;

public abstract class SourceSupport<T> implements Serializable {

    public abstract T poll() throws Exception;

    public void init(Processor.Context context) throws Exception {
        //intentionally no-op
    }


    private final static class MySource extends AbstractProcessor implements Serializable {
        private final SourceSupport sourceSupport;
        private Object pendingItem;

        private MySource(SourceSupport sourceSupport) {
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

    public final Source<T> asSource() {
        ProcessorMetaSupplier processorMetaSupplier = dontParallelize(
                cloneAndSupply(new MySource(this)));

        return Sources.fromProcessor(this.toString(), processorMetaSupplier);
    }
}
