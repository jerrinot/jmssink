package info.jerrinot.o2.common;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import org.junit.Test;

import java.util.function.Function;

import static com.hazelcast.jet.pipeline.Sinks.logger;
import static org.junit.Assert.*;

public class AsyncSourceSupportTest {

    @Test
    public void foo() {
        Pipeline pipeline = Pipeline.create();

        pipeline.drawFrom(new MyRandomSource().asSource())
                .drainTo(logger());

        JetInstance jetInstance = Jet.newJetInstance();
        Job job = jetInstance.newJob(pipeline);

        job.join();
    }

    public static class MyRandomSource implements AsyncSourceSupport<Long> {
        @Override
        public void start(Processor.Context context, Function<Long, Boolean> consumer) {
            new Thread(() -> {
                for (long i = 0;;i++) {
                    consumer.apply(i);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
    }

}