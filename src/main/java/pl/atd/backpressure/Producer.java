package pl.atd.backpressure;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;

import java.util.concurrent.Callable;
import java.util.function.BiFunction;

@Component
public class Producer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);
    private Counter producerCounter = Metrics.counter("producer");

    public Flux<Long> produce(int targetRate, long upto) {

        long delayBetweenEmits = 1000L / targetRate;

        // To supply an initial state
        Callable<Long> initialState = () -> 1L;

        // Flux which accepts initial state and bifunction
        return Flux.generate(initialState, generator(upto, delayBetweenEmits));
    }

    /**
     * Generator BiFunction to consume the state, emit value, change state
     * @param upto number of events
     * @param delayBetweenEmits delay between events
     * @return generator BiFunction
     */
    private BiFunction<Long, SynchronousSink<Long>, Long> generator(long upto, long delayBetweenEmits) {
        return (state, sink) -> {

            try {
                Thread.sleep(delayBetweenEmits);
            } catch (InterruptedException e) {
                LOGGER.error("Sleep interrupted", e);
            }

            long nextState = state + 1;
            if (state > upto) {
                LOGGER.info("Completed");
                sink.complete();
            } else {
                LOGGER.info("Emitted {}", state);
                sink.next(state);
                producerCounter.increment();
            }
            return nextState;
        };
    }
}
