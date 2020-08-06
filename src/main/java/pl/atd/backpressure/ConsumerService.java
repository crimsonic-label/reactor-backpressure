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

    public Mono<ServerResponse> handleMessageScenario1(ServerRequest request) {
        long count = Long.parseLong(request.queryParam("count").orElse("100"));
        int producerRate = Integer.parseInt(request.queryParam("producerRate").orElse("5"));
        int consumerRate = Integer.parseInt(request.queryParam("consumerRate").orElse("5"));
        long delayBetweenConsumes = 1000L / consumerRate;

        producer.produce(producerRate, count).subscribe(consumer(delayBetweenConsumes));

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
