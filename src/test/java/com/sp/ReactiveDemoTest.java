package com.sp;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.tools.agent.ReactorDebugAgent;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ReactiveDemoTest {

    private ReactiveDemo demo = new ReactiveDemo();

    @Test
    void fluxJust() {
        var stringFlux = demo.fluxJust().log();
        StepVerifier.create(stringFlux)
                .expectNext("aman", "aniket", "sonhan", "nikhil", "amle", "nikhil")
                .verifyComplete();
    }

    @Test
    void fluxIterable() {
        var stringFlux = demo.fluxIterable().log();
        StepVerifier.create(stringFlux)
                .expectNext("aman", "aniket", "sonhan", "nikhil", "amle", "nikhil")
                .verifyComplete();
    }

    @Test
    void monoJust() {
        var stringFlux = demo.monoJust().log();
        StepVerifier.create(stringFlux)
                .expectNext("aman")
                .verifyComplete();
    }

    @Test
    void flux_map() {
        var stringFlux = demo.flux_map().log();
        StepVerifier.create(stringFlux)
                .expectNext("AMAN", "ANIKET", "SONHAN", "NIKHIL", "AMLE", "NIKHIL")
                .verifyComplete();
    }

    @Test
    void stream_immutable() {
        var stringFlux = demo.stream_immutable().log();
        StepVerifier.create(stringFlux)
                .expectNext("aman", "aniket", "sonhan", "nikhil", "amle", "nikhil")
                .verifyComplete();
    }

    @Test
    void flux_filter() {
        var stringFlux = demo.flux_filter().log();
        StepVerifier.create(stringFlux)
                .expectNext("aniket", "sonhan", "nikhil")
                .verifyComplete();
    }

    @Test
    void flux_distinct() {
        var stringFlux = demo.flux_distinct().log();
        StepVerifier.create(stringFlux)
                .expectNextCount(5)
                .verifyComplete();
    }

    @Test
    void flux_flatmap() {
        var stringFlux = demo.flux_flatmap().log();
        StepVerifier.create(stringFlux)
                .expectNext("a","m","a","n","a","n","i","k","e","t")
                .verifyComplete();
    }

    @Test
    void flux_flatmap_async() {
        var stringFlux = demo.flux_flatmap_async().log();
        StepVerifier.create(stringFlux)
                .expectNextCount(10)
                .verifyComplete();
    }

    @Test
    void flux_concatMap() {
        var stringFlux = demo.flux_concatMap().log();
        StepVerifier.create(stringFlux)
                .expectNext("a","m","a","n","a","n","i","k","e","t")
                .verifyComplete();
    }

    @Test
    void flux_flatMapSequential() {
        var stringFlux = demo.flux_flatMapSequential().log();
        StepVerifier.create(stringFlux)
                .expectNext("a","m","a","n","a","n","i","k","e","t")
                .verifyComplete();
    }

    @Test
    void flux_flatMapMany() {
        var monoList = Mono.just(List.of("sonhan", "nikhil", "amle", "nikhil"));
        var stringFlux = demo.flux_flatMapMany(monoList).log();
        StepVerifier.create(stringFlux)
                .expectNext("sonhan", "nikhil", "amle", "nikhil")
                .verifyComplete();
    }

    @Test
    void flux_flatMapIterable() {
        var monoList = Mono.just(List.of("sonhan", "nikhil", "amle", "nikhil"));
        var stringFlux = demo.flux_flatMapIterable(monoList).log();
        StepVerifier.create(stringFlux)
                .expectNext("sonhan", "nikhil", "amle", "nikhil")
                .verifyComplete();
    }

    @Test
    void flux_transform() {
        var stringFlux = demo.flux_transform().log();
        StepVerifier.create(stringFlux)
                .expectNext("ANIKET", "SONHAN", "NIKHIL")
                .verifyComplete();
    }

    @Test
    void flux_defaultIfEmpty() {
        var stringFlux = demo.flux_defaultIfEmpty("j").log();
        StepVerifier.create(stringFlux)
                .expectNext("Not found")
                .verifyComplete();
    }

    @Test
    void flux_switchIfEmpty() {
        var stringFlux = demo.flux_switchIfEmpty("j").log();
        StepVerifier.create(stringFlux)
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void flux_concat() {
        var stringFlux = demo.flux_concat(Flux.just("aman", "mohit"),
                Flux.just("jr. kuro", "mh. muru"), Flux.just("manoj", "jay")).log();
        StepVerifier.create(stringFlux)
                .expectNext("aman", "mohit", "jr. kuro")
                .expectNextCount(3)
                .verifyComplete();
    }

    @Test
    void flux_concatWith() {
        var stringFlux = demo.flux_concatWith(Flux.just("aman", "mohit"),
                Flux.just("jr. kuro", "mh. muru"), Flux.just("manoj", "jay")).log();
        StepVerifier.create(stringFlux)
                .expectNext("aman", "mohit", "manoj")
                .expectNextCount(3)
                .verifyComplete();
    }

    @Test
    void flux_merge() {
        var stringFlux = demo.flux_merge(Flux.just("aman", "mohit"),
                Flux.just("jr. kuro", "mh. muru"), Flux.just("manoj", "jay")).log();
        StepVerifier.create(stringFlux)
                .expectNextCount(6)
                .verifyComplete();
    }

    @Test
    void flux_mergeWith() {
        var stringFlux = demo.flux_mergeWith(Flux.just("aman", "mohit"),
                Flux.just("jr. kuro", "mh. muru"), Flux.just("manoj", "jay")).log();
        StepVerifier.create(stringFlux)
                .expectNextCount(6)
                .verifyComplete();
    }

    @Test
    void flux_mergeSequential() {
        var stringFlux = demo.flux_mergeSequential(Flux.just("aman", "mohit"),
                Flux.just("jr. kuro", "mh. muru"), Flux.just("manoj", "jay")).log();
        StepVerifier.create(stringFlux)
                .expectNext("aman", "mohit", "jr. kuro", "mh. muru", "manoj", "jay")
                .verifyComplete();
    }

    @Test
    void flux_zip() {
        var stringFlux = demo.flux_zip(Flux.just("amit", "aman"), Flux.just("gupta", "goyal")).log();
        StepVerifier.create(stringFlux)
                .expectNext("AMIT GUPTA", "AMAN GOYAL")
                .verifyComplete();
    }

    @Test
    void flux_zipWith() {
        var stringFlux = demo.flux_zip(Flux.just("ankit", "aman"), Flux.just("gupta", "goyal")).log();
        StepVerifier.create(stringFlux)
                .expectNext("ANKIT GUPTA", "AMAN GOYAL")
                .verifyComplete();
    }

    @Test
    void flux_doOnCallbacks() {
        var stringFlux = demo.flux_doOnCallbacks();
        StepVerifier.create(stringFlux)
                .expectNext("A", "B", "C")
                .verifyComplete();
    }

    @Test
    void flux_onErrorReturn() {
        var stringFlux = demo.flux_onErrorReturn().log();
        StepVerifier.create(stringFlux)
                .expectNext("A", "B", "E")
                .verifyComplete();
    }

    @Test
    void flux_onErrorResume() {
        var stringFlux = demo.flux_onErrorResume().log();
        StepVerifier.create(stringFlux)
                .expectNext("A", "B", "D")
                .verifyComplete();
    }

    @Test
    void flux_onErrorContinue() {
        var stringFlux = demo.flux_onErrorContinue().log();
        StepVerifier.create(stringFlux)
                .expectNext("A", "C")
                .verifyComplete();
    }

    @Test
    void flux_onErrorMap() {
        var stringFlux = demo.flux_onErrorMap().log();
        StepVerifier.create(stringFlux)
                .expectNext("A")
                .expectError(CustomException.class)
                .verify();
    }

    @Test
    void flux_doOnError() {
        var stringFlux = demo.flux_doOnError().log();
        StepVerifier.create(stringFlux)
                .expectNext("A")
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    void flux_retry() {
        var stringFlux = demo.flux_retry().log();
        StepVerifier.create(stringFlux)
                .expectNextCount(Long.MAX_VALUE)
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    void flux_retryNtimes() {
        var stringFlux = demo.flux_retryNtimes().log();
        StepVerifier.create(stringFlux)
                .expectNext("A")
                .expectNextCount(3)
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    void flux_retryWhen_IllegalArgumentException() {
        var demoExp = new IllegalArgumentException("Exception occurred dummy!!");
        var stringFlux = demo.flux_retryWhen(demoExp).log();
        StepVerifier.create(stringFlux)
                .expectNext("A")
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    void flux_retryWhen_CustomException() {
        var customDemoExp = new CustomException("Custom exception occurred dummy!!");
        var stringFlux = demo.flux_retryWhen(customDemoExp).log();
        StepVerifier.create(stringFlux)
                .expectNext("A")
                .expectNextCount(3)
                .expectError(CustomException.class)
                .verify();
    }

    @Test
    void flux_repeat() {
        var stringFlux = demo.flux_repeat().log();
        StepVerifier.create(stringFlux)
                .expectNextCount(6)
                .thenCancel()
                .verify();
    }

    @Test
    void flux_repeatWithN() {
        int num = 2;
        var stringFlux = demo.flux_repeatWithN(num).log();
        StepVerifier.create(stringFlux)
                .expectNextCount(9)
                .verifyComplete();
    }

    @Test
    void flux_publishOn() {
        var stringFlux = demo.flux_publishOn();
        StepVerifier.create(stringFlux)
                .expectNextCount(6)
                .verifyComplete();
    }

    @Test
    void flux_subscribeOn() {
        var stringFlux = demo.flux_subscribeOn();
        StepVerifier.create(stringFlux)
                .expectNextCount(6)
                .verifyComplete();
    }

    @Test
    void explore_parallelflux() {
        var stringFlux = demo.explore_parallelflux();
        StepVerifier.create(stringFlux)
                .expectNextCount(3)
                .verifyComplete();
    }

    @Test
    void explore_flatmap() {
        var stringFlux = demo.explore_flatmap();
        StepVerifier.create(stringFlux)
                .expectNextCount(3)
                .verifyComplete();
    }

    @Test
    void explore_flatMapSequential() {
        var stringFlux = demo.explore_flatMapSequential();
        StepVerifier.create(stringFlux)
                .expectNextCount(3)
                .verifyComplete();
    }

    //To make faster your code analysing time.
    @Test
    void flux_concatMap_virtualTime() {
        VirtualTimeScheduler.getOrSet();
        var stringFlux = demo.flux_concatMap().log();
        StepVerifier.withVirtualTime(() -> stringFlux)
                .thenAwait(Duration.ofSeconds(5))
                .expectNext("a","m","a","n","a","n","i","k","e","t")
                .verifyComplete();
    }

    @Test
    void flux_onErrorMap_HooksOnOperatorDebug() {
        //Hooks.onOperatorDebug(); impact app performance.
        // instead of Hooks.onOperatorDebug(); use checkpoint() but not descriptive.
        //Recommanded approach for prod env:
        // i) run alongwith app,
        // ii) collecting all stack trace info of each operator in background.
        // iii) not impact the app performance.
        //In Spring Boot use in main(..) before to start app for more see reactor guide.
        ReactorDebugAgent.init();
        ReactorDebugAgent.processExistingClasses();
        var ex = new IllegalStateException("Error my me");
        var stringFlux = demo.flux_onErrorMap_HooksOnOperatorDebug(ex).log();
        StepVerifier.create(stringFlux)
                .expectError(IllegalStateException.class)
                .verify();
    }
}