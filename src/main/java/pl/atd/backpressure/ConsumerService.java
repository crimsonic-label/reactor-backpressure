package pl.atd.backpressure;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

@Service
public class ConsumerService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerService.class);
    private ExecutorService executorService = Executors.newFixedThreadPool(5);
    private Counter consumerCounter = Metrics.counter("consumer");

    @Autowired
    private Producer producer;

    /**
     * Slow Consumer without Threading
     * @param request reactive web request
     * @return mono with accepted status
     */
    public Mono<ServerResponse> handleMessageScenario1(ServerRequest request) {
        long count = Long.parseLong(request.queryParam("count").orElse("100"));
        int producerRate = Integer.parseInt(request.queryParam("producerRate").orElse("5"));
        int consumerRate = Integer.parseInt(request.queryParam("consumerRate").orElse("1"));
        long delayBetweenConsumes = 1000L / consumerRate;

        // producer and the consumer chained together
        producer.produce(producerRate, count).subscribe(consumer(delayBetweenConsumes));

        return ServerResponse.accepted().build();
    }

    public Mono<ServerResponse> handleMessageScenario2(ServerRequest request) {
        return ServerResponse.accepted().build();
    }

    public Mono<ServerResponse> handleMessageScenario3(ServerRequest request) {
        return ServerResponse.accepted().build();
    }

    private Consumer<Long> consumer(long delayBetweenConsumes) {
        return (Long value) -> {
            try {
                Thread.sleep(delayBetweenConsumes);
            } catch (InterruptedException e) {
                LOGGER.error("Sleep interrupted", e);
            }
            LOGGER.info("Consumerd: {}", value);
            consumerCounter.increment();
        };
    }
}
