package pl.atd.backpressure;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;

import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;

@Configuration
public class RoutesConfig {

    @Bean
    public RouterFunction<?> routerFunction(ConsumerService consumer) {
        return route(GET("/scenario1"), consumer::handleMessageScenario1)
                .and(route(GET("/scenario2"), consumer::handleMessageScenario2))
                .and(route(GET("/scenario3"), consumer::handleMessageScenario3))
                .and(route(GET("/scenario4"), consumer::handleMessageScenario4));
    }
}
