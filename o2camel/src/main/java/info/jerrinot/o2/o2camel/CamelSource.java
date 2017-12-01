package info.jerrinot.o2.o2camel;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.DistributedSupplier;
import info.jerrinot.o2.impl.SourceSupport;
import org.apache.camel.CamelContext;
import org.apache.camel.Component;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

public class CamelSource<T> extends SourceSupport<T> {
    private final Pair<String, DistributedSupplier<Component>> initializer;
    private final String fromRoute;

    private transient BlockingQueue<T> queue;

    public CamelSource(Pair<String, DistributedSupplier<Component>> initializer, String fromRoute) {
        this.initializer = initializer;
        this.fromRoute = fromRoute;
    }

    public void configureRoutes(RouteBuilder routeBuilder) {
//        routeBuilder.from(fromRoute).to("jet-local");
        routeBuilder.rest("/hello")
                .get("/world").to("jet-local");
//                .get ("/world").to("direct:world").responseMessage().endResponseMessage();

//        routeBuilder.from("jet-local").transform().constant("Hello World");
    }

    @Override
    public void init(Processor.Context ignored) throws Exception {
        CamelContext context = new DefaultCamelContext();
        context.addComponent(initializer.getLeft(), initializer.getRight().get());
        queue = new LinkedBlockingQueue<>(Integer.MAX_VALUE);
        context.addComponent("jet-local", new JetComponent(queue));



        context.addRoutes(new RouteBuilder() {
            public void configure() {
                CamelSource.this.configureRoutes(this);
            }
        });

        context.start();
    }

    @Override
    public T poll() throws Exception {
        return queue.poll();
    }
}
