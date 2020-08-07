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
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

@Service
public class ConsumerService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerService.class);
    private ExecutorService executorService = Executors.newFixedThreadPool(5);
    private Scheduler publishOnScheduler = Schedulers.newBoundedElastic(5, 10, "publish");
    private Scheduler subscribeOnScheduler = Schedulers.newBoundedElastic(5, 10, "subscribe");
    private Scheduler flatMapScheduler = Schedulers.newBoundedElastic(5, 10, "flatMap");

    private Counter consumerCounter = Metrics.counter("consumer");

    @Autowired
    private Producer producer;

    /**
     * Slow Consumer without Threading - everything in one thread
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

    /**
     * Producer and the Consumer being produced independently in different threads (one for producer, one for consumer)
     * backpressure appear at large value of records - buffering requests in publishOn tops new requests
     * change prefetch value to observe
     */
    public Mono<ServerResponse> handleMessageScenario2(ServerRequest request) {
        long count = Long.parseLong(request.queryParam("count").orElse("100"));
        int producerRate = Integer.parseInt(request.queryParam("producerRate").orElse("5"));
        int consumerRate = Integer.parseInt(request.queryParam("consumerRate").orElse("1"));
        long delayBetweenConsumes = 1000L / consumerRate;
        int prefetch = request.queryParam("prefetch").map(Integer::parseInt).orElse(Queues.SMALL_BUFFER_SIZE);

        producer.produce(producerRate, count)
                // subscribe on changes the thread where producer produces
                .subscribeOn(subscribeOnScheduler)
                // publish on changes the thread where consumptions occurs
                .publishOn(publishOnScheduler, prefetch)
                .subscribe(consumer(delayBetweenConsumes));

        return ServerResponse.accepted().build();
    }

    /**
     * Multi-threaded Consumer
     */
    public Mono<ServerResponse> handleMessageScenario3(ServerRequest request) {
        long count = Long.parseLong(request.queryParam("count").orElse("100"));
        int producerRate = Integer.parseInt(request.queryParam("producerRate").orElse("5"));
        int consumerRate = Integer.parseInt(request.queryParam("consumerRate").orElse("1"));
        long delayBetweenConsumes = 1000L / consumerRate;
        int prefetch = request.queryParam("prefetch").map(Integer::parseInt).orElse(Queues.SMALL_BUFFER_SIZE);
        int concurrency = request.queryParam("concurrency").map(Integer::parseInt).orElse(5);

        producer.produce(producerRate, count)
                // subscribe on changes the thread where producer produces
                .subscribeOn(subscribeOnScheduler)
                // publish on changes the thread where consumptions occurs
                .publishOn(publishOnScheduler, prefetch)
                .flatMap((value) -> Mono.fromSupplier(() -> {
                    try {
                        Thread.sleep(delayBetweenConsumes);
                    } catch (InterruptedException e) {
                        LOGGER.error("Sleep interrupted", e);
                    }
                    LOGGER.info("Consumed: {}", value);
                    consumerCounter.increment();
                    return null;
                }), concurrency)
                .subscribe();

        return ServerResponse.accepted().build();
    }

    private Consumer<Long> consumer(long delayBetweenConsumes) {
        return (Long value) -> {
            try {
                Thread.sleep(delayBetweenConsumes);
            } catch (InterruptedException e) {
                LOGGER.error("Sleep interrupted", e);
            }
            LOGGER.info("Consumed: {}", value);
            consumerCounter.increment();
        };
    }
}
