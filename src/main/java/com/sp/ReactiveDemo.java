package com.sp;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.publisher.*;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class ReactiveDemo {

    public Flux<String> fluxJust() {
        var fluxNames = Flux.just("aman", "aniket", "sonhan", "nikhil", "amle", "nikhil");
        return fluxNames;
    }

    public Flux<String> fluxIterable() {
        var fluxNames = Flux.fromIterable(List.of("aman", "aniket", "sonhan", "nikhil", "amle", "nikhil"));
        return fluxNames;
    }

    public Mono<String> monoJust() {
        var monoName = Mono.just("aman");
        return monoName;
    }

    public Flux<String> flux_map() {
        var fluxNames = Flux.just("aman", "aniket", "sonhan", "nikhil", "amle", "nikhil")
                .map(String::toUpperCase);
        return fluxNames;
    }

    public Flux<String> stream_immutable() {
        var fluxNames = Flux.just("aman", "aniket", "sonhan", "nikhil", "amle", "nikhil");
        fluxNames.map(String::toUpperCase);
        return fluxNames;
    }

    public Flux<String> flux_filter() {
        var fluxNames = Flux.just("aman", "aniket", "sonhan", "nikhil", "amle")
                .filter(name -> name.length() > 4);
        return fluxNames;
    }

    public Flux<String> flux_distinct() {
        var fluxNames = Flux.just("aman", "aniket", "sonhan", "nikhil", "amle", "nikhil")
                .distinct();
        return fluxNames;
    }

    public Flux<String> flux_flatmap() {
        var fluxNames = Flux.just("aman", "aniket")
                .flatMap(nm -> splitNameChars(nm));
        return fluxNames;
    }

    public Flux<String> flux_flatmap_async() {
        var fluxNames = Flux.just("aman", "aniket")
                .flatMap(nm -> splitNameCharsWithDelay(nm));
        return fluxNames;
    }

    public Flux<String> flux_concatMap() {
        var fluxNames = Flux.just("aman", "aniket")
                .concatMap(nm -> splitNameCharsWithDelay(nm));
        return fluxNames;
    }

    public Flux<String> flux_flatMapSequential() {
        var fluxNames = Flux.just("aman", "aniket")
                .flatMapSequential(nm -> splitNameCharsWithDelay(nm));
        return fluxNames;
    }

    public Flux<String> flux_flatMapMany(Mono<List<String>> monoList) {
        var fluxNames = monoList.flatMapMany(list -> Flux.fromIterable(list));
        return fluxNames;
    }

    public Flux<String> flux_flatMapIterable(Mono<List<String>> monoList) {
        //flatMapIterable very optimized as compare to flatMapMany.
        var fluxNames = monoList.flatMapIterable(t -> t);
        return fluxNames;
    }

    public Flux<String> flux_transform() {
        var fluxNames = Flux.just("aman", "aniket", "sonhan", "nikhil", "amle", "nikhil")
                .transform(this::filterMapDistinct);
        return fluxNames;
    }

    public Flux<String> flux_defaultIfEmpty(String startWithStr) {
        var fluxNames = Flux.just("aman", "aniket", "sonhan", "nikhil", "amle", "nikhil")
                .filter(nm -> nm.startsWith(startWithStr))
                .defaultIfEmpty("Not found");
        return fluxNames;
    }

    public Flux<String> flux_switchIfEmpty(String startWithStr) {
        var fluxNames = Flux.just("aman", "aniket", "sonhan", "nikhil", "amle", "nikhil")
                .filter(nm -> nm.startsWith(startWithStr))
                .switchIfEmpty(fallback(startWithStr));
        return fluxNames;
    }

    public Flux<String> flux_concat(Flux<String> students, Flux<String> teachers, Publisher<String> other_staff) {
        var fluxNames = Flux.concat(students, teachers.delayElements(Duration.ofMillis(200)), other_staff);
        return fluxNames;
    }

    public Flux<String> flux_concatWith(Flux<String> students, Flux<String> teachers, Publisher<String> other_staff) {
        var fluxNames = students.concatWith(other_staff).concatWith(teachers);
        return fluxNames;
    }

    public Flux<String> flux_merge(Flux<String> students, Flux<String> teachers, Publisher<String> other_staff) {
        var fluxNames = Flux.merge(students, teachers.delayElements(Duration.ofMillis(200)), other_staff);
        return fluxNames;
    }

    public Flux<String> flux_mergeWith(Flux<String> students, Flux<String> teachers, Publisher<String> other_staff) {
        var fluxNames = students.delayElements(Duration.ofMillis(200)).mergeWith(teachers.delayElements(Duration.ofMillis(100)))
                .concatWith(other_staff);
        return fluxNames;
    }

    public Flux<String> flux_mergeSequential(Flux<String> students, Flux<String> teachers, Publisher<String> other_staff) {
        var fluxNames = Flux.mergeSequential(students.delayElements(Duration.ofMillis(200)),
                teachers.delayElements(Duration.ofMillis(100)), other_staff);
        return fluxNames;
    }

    public Flux<String> flux_zip(Flux<String> names, Flux<String> surnames) {
        var fluxNames = Flux.zip(names, surnames, (n,s) -> (n+" "+s).toUpperCase());
        return fluxNames;
    }

    public Flux<String> flux_zipWith(Flux<String> names, Flux<String> surnames) {
        var fluxNames = names.zipWith(surnames, (n,s) -> (n+" "+s).toUpperCase());
        return fluxNames;
    }

    public Flux<String> flux_doOnCallbacks() {
        var fluxNames = Flux.just("A", "B", "C")
                .doOnSubscribe(s -> System.out.println("SUBSCRIBED : " + s))
                .doOnNext(i -> System.out.println("RECEIVED VALUE: " + i))
                .doOnError(e -> System.out.println("ERROR OCCURED: " + e.getMessage()))
                .doOnComplete(() -> {
                    System.out.println("COMPLETION DONE");
                }).doOnRequest(l -> System.out.println("Request Count: " + l));
        return fluxNames;
    }

    //Recovery exception methods.
    public Flux<String> flux_onErrorReturn() {
        var fluxNames = Flux.just("A", "B", "C")
                .map(v -> {
                    if (v.equals("C"))
                        throw new IllegalArgumentException("Error occurred");
                    return v;
                })
                .onErrorReturn("E");
        return fluxNames;
    }

    public Flux<String> flux_onErrorResume() {
        var fallbackMono = Mono.just("D");
        var fluxNames = Flux.just("A", "B", "C")
                .map(v -> {
                    if (v.equals("C"))
                        throw new IllegalArgumentException("Error occurred");
                    return v;
                })
                .onErrorResume(t -> {
                    System.out.println("Error : " + t.getMessage());
                    return fallbackMono;
                });
        return fluxNames;
    }

    public Flux<String> flux_onErrorContinue() {
        var fallbackMono = Mono.just("D");
        var fluxNames = Flux.just("A", "B", "C")
                .map(v -> {
                    if (v.equals("B"))
                        throw new IllegalArgumentException("Error occurred");
                    return v;
                })
                .onErrorContinue((t,v) -> {
                    System.out.println("Error : " + t.getMessage());
                    System.out.println("Cause value : " + v);
                });
        return fluxNames;
    }

    //Transform exception into business exception.
    public Flux<String> flux_onErrorMap() {
        var fluxNames = Flux.just("A", "B", "C")
                .map(v -> {
                    if (v.equals("B"))
                        throw new IllegalArgumentException("Error occurred");
                    return v;
                })
                .onErrorMap(t -> {
                    System.out.println("Error : " + t.getMessage());
                    return new CustomException(t.getMessage());
                });
        return fluxNames;
    }

    public Flux<String> flux_doOnError() {
        var fluxNames = Flux.just("A", "B", "C")
                .map(v -> {
                    if (v.equals("B"))
                        throw new IllegalArgumentException("Error occurred");
                    return v;
                })
                .doOnError(t -> {
                    System.out.println("Error : " + t.getMessage());
                });
        return fluxNames;
    }

    //Retry if execution failure.
    public Flux<String> flux_retry() {
        var fluxNames = Flux.just("A", "B", "C")
                .map(v -> {
                    if (v.equals("B"))
                        throw new IllegalArgumentException("Error occurred");
                    return v;
                })
                .retry();
        return fluxNames;
    }

    public Flux<String> flux_retryNtimes() {
        var fluxNames = Flux.just("A", "B", "C")
                .map(v -> {
                    if (v.equals("B"))
                        throw new IllegalArgumentException("Error occurred");
                    return v;
                })
                .retry(3);
        return fluxNames;
    }

    public Flux<String> flux_retryWhen(RuntimeException exp) {
        Retry retry = Retry.fixedDelay(3, Duration.ofMillis(500))
                .filter(ex -> ex instanceof CustomException)
                .doAfterRetry(retrySignal -> {
                    System.out.println("Completed Retry : " + retrySignal.totalRetries());
                }).onRetryExhaustedThrow((backoffSpec, retrySignal) -> Exceptions.propagate(retrySignal.failure()));
        var fluxNames = Flux.just("A", "B", "C")
                .map(v -> {
                    if (v.equals("B"))
                        throw exp;
                    return v;
                })
                .retryWhen(retry);
        return fluxNames;
    }

    public Flux<String> flux_repeat() {
        var fluxNames = Flux.just("A", "B", "C")
                .map(String::toLowerCase)
                .repeat();
        return fluxNames;
    }

    public Flux<String> flux_repeatWithN(int n) {
        var fluxNames = Flux.just("A", "B", "C")
                .map(String::toLowerCase)
                .repeat(n);
        return fluxNames;
    }

    //Reactive Execution Model - publishOn(..) To apply parallelism into further downstream.
    public Flux<String> flux_publishOn() {
        List<String> names = List.of("aman", "mohit", "chru");
        List<String> names1 = List.of("gopal", "dharmu", "kapil");
        var fluxNames = getFluxNames(names)
                .map(this::toUppercase).log();
        var fluxNames1 = getFluxNames(names1)
                .map(this::toUppercase)
                .log();
        return fluxNames.mergeWith(fluxNames1);
    }

    //Reactive Execution Model - subscribeOn(..) To apply parallelism into whole reactive stream.
    // use in case influence upstream of third party library.
    public Flux<String> flux_subscribeOn() {
        List<String> names = List.of("aman", "mohit", "chru");
        List<String> names1 = List.of("gopal", "dharmu", "kapil");
        var fluxNames = getFluxNames(names).subscribeOn(Schedulers.parallel()).log();
        var fluxNames1 = getFluxNames(names1).subscribeOn(Schedulers.parallel()).log();
        return fluxNames.mergeWith(fluxNames1);
    }

    //Achieve parallelism by different ways.
    public ParallelFlux<String> explore_parallelflux() {
        List<String> names = List.of("aman", "mohit", "chru");
        var fluxNames = Flux.fromIterable(names)
                .parallel()
                .runOn(Schedulers.parallel())
                .map(this::toUppercase)
                .map(s -> s + " : " + s.length())
                .log();
        return fluxNames;
    }

    //Achieve parallelism along with ordering.
    public Flux<String> explore_flatmap() {
        List<String> names = List.of("aman", "mohit", "chru");
        var fluxNames = Flux.fromIterable(names)
                .flatMap(obj -> Mono.just(obj).map(this::toUppercase)
                        .subscribeOn(Schedulers.parallel()))
                .map(s -> s + " : " + s.length())
                .log();
        return fluxNames;
    }

    public Flux<String> explore_flatMapSequential() {
        List<String> names = List.of("aman", "mohit", "chru");
        var fluxNames = Flux.fromIterable(names)
                .flatMapSequential(obj -> Mono.just(obj).map(this::toUppercase)
                        .subscribeOn(Schedulers.parallel()))
                .map(s -> s + " : " + s.length())
                .log();
        return fluxNames;
    }

    // Drop the other elements which are not requested by subscriber. keep the track of drop elements. (convert subscriber 'n' requests into unbounded)
    public static void explore_onBackpressureDrop() {
        Flux.range(1, 100)
                .onBackpressureDrop(i -> {
                    System.out.println("Drop item :: "  + i);
                }).log().subscribe(new BaseSubscriber<Integer>() {
                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                        subscription.request(5);
                    }

                    @Override
                    protected void hookOnNext(Integer value) {
                        if (value%5 == 0 && value < 50) {
                            request(5);
                        }
                    }

                    @Override
                    protected void hookOnComplete() {
                        System.out.println("Complete signal invoked!!");
                    }

                    @Override
                    protected void hookOnCancel() {
                        cancel();
                    }
                });
    }

    // Buffer the maxSize of elements and invoke onOverflow callback when ever happen. (convert subscriber 'n' requests into unbounded)
    public static void explore_onBackpressureBuffer() {
        Flux.range(1, 100)
                .log()
                .onBackpressureBuffer(20, i -> {
                    System.out.println("Last buffered item : " + i);
                }).subscribe(new BaseSubscriber<Integer>() {
                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                        subscription.request(1);
                    }

                    @Override
                    protected void hookOnNext(Integer value) {
                        if (value < 50) {
                            request(1);
                        }
                        System.out.println("RECEIVED : " + value);
                    }

                    @Override
                    protected void hookOnComplete() {
                        System.out.println("Complete signal invoked!!");
                    }

                    @Override
                    protected void hookOnCancel() {
                        System.out.println("Cancel invoking item");
                    }
                });
    }

    // OverflowException - If queue received more than requesting. (convert subscriber 'n' requests into unbounded)
    public static void explore_onBackpressureError() {
        Flux.range(1, 100)
                .log()
                .onBackpressureError().subscribe(new BaseSubscriber<Integer>() {
                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                        subscription.request(1);
                    }

                    @Override
                    protected void hookOnNext(Integer value) {
                        if (value < 50) {
                            request(1);
                        }
                        System.out.println("RECEIVED : " + value);
                    }

                    @Override
                    protected void hookOnComplete() {
                        System.out.println("Complete signal invoked!!");
                    }

                    @Override
                    protected void hookOnCancel() {
                        System.out.println("Cancel invoking item");
                    }
                });
    }

    //Cold stream: Cold stream is also a kind stream which we saw so far.
    // this stream is provide the data from beginning to end to all subscriber.
    public static void cold_stream() {
        Flux<Integer> fluxInteger = Flux.range(1, 10);

        fluxInteger.subscribe(i -> System.out.println("Subscriber - 1: " + i));
        sleepSeconds(2);
        fluxInteger.subscribe(i -> System.out.println("Subscriber - 2: " + i));
    }

    //Hot stream: kind of stream, in this stream the subscriber only receive the current state of the stream.
    // Two kind of hot stream: i) With subscribe ii) Without subscribe.
    // publish: Provide ConnectableFlux as return type and before to start the stream IMPORTANT: invoke connect().
    public static void hot_stream() {
        Flux<Integer> fluxInteger = Flux.range(1, 10)
                .delayElements(Duration.ofSeconds(1));

        ConnectableFlux<Integer> connectableFlux = fluxInteger.publish();
        connectableFlux.connect();
        connectableFlux.subscribe(i -> System.out.println("Subscriber - 1: " + i));
        sleepSeconds(4);
        connectableFlux.subscribe(i -> System.out.println("Subscriber - 2: " + i));
        sleepSeconds(10);
    }

    // autoConnect: Use to automatically connect with stream when min subscriber available.
    public static void hot_stream_autoConnect() {
        Flux<Integer> fluxInteger = Flux.range(1, 10)
                .delayElements(Duration.ofSeconds(1));

        Flux<Integer> fluxNew = fluxInteger.publish().autoConnect(2);
        fluxNew.subscribe(i -> System.out.println("Subscriber - 1: " + i));
        sleepSeconds(2);
        fluxNew.subscribe(i -> System.out.println("Subscriber - 2: " + i));
        System.out.println("Two subscriber available NOW!!");
        sleepSeconds(2);
        fluxNew.subscribe(i -> System.out.println("Subscriber - 3: " + i));
        sleepSeconds(10);
    }

    // RefCount: keep the track of available min subscribers to start the stream and if no subscriber then cancel the stream.
    public static void hot_stream_refConnect() {
        Flux<Integer> fluxInteger = Flux.range(1, 10)
                .delayElements(Duration.ofSeconds(1)).doOnCancel(() -> System.out.println("Cancelled execution!!"));

        var fluxNew = fluxInteger.publish().refCount(2);
        var disposable1 = fluxNew.subscribe(i -> System.out.println("Subscriber - 1: " + i));
        sleepSeconds(2);
        var disposable2 = fluxNew.subscribe(i -> System.out.println("Subscriber - 2: " + i));
        System.out.println("Two subscriber available NOW!!");
        sleepSeconds(4);
        disposable1.dispose();
        disposable2.dispose();
        sleepSeconds(2);
        fluxNew.subscribe(i -> System.out.println("Subscriber - 3: " + i));
        System.out.println("Subscriber - 3 available NOW!!");
        sleepSeconds(2);
        fluxNew.subscribe(i -> System.out.println("Subscriber - 4: " + i));
        System.out.println("Two subscriber available NOW!!");
        sleepSeconds(10);
        System.out.println("End execution!!");
    }

    public Flux<Object> flux_onErrorMap_HooksOnOperatorDebug(RuntimeException e) {
        return Flux.just("A", "B", "C")
                .flatMap(v -> Flux.error(e))
                //.checkpoint("flux_onErrorMap_HooksOnOperatorDebug in flatMap")
                .filter(v -> {
                    return v != "B";
                });
                /*.onErrorMap(t -> {
                    System.out.println("Error : " + t.getMessage());
                    return new CustomException(t.getMessage());
                });*/
    }

    public static void main(String[] args) {
        hot_stream_refConnect();
    }

//----------PRIVATE METHODS-----------------------------------------------------------------------------

    private Flux<String> getFluxNames(List<String> names) {
        return Flux.fromIterable(names).map(this::toUppercase);
    }

    private String toUppercase(String str) {
        sleepSeconds(1);
        return str.toUpperCase();
    }

    private static void sleepSeconds(int second) {
        try {
            TimeUnit.SECONDS.sleep(second);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private Flux<String> fallback(String achar) {
        return Flux.just("jhon", "kalam", "umenkey").filter(nm -> nm.startsWith(achar));
    }

    private Flux<String> filterMapDistinct(Flux<String> fluxNames) {
        return fluxNames.filter(nm -> nm.length() > 4).map(String::toUpperCase).distinct();
    }

    private Function<Flux<String>, Flux<String>> filterMapDistinct() {
        return fluxNames -> fluxNames.filter(nm -> nm.length() > 4).map(String::toUpperCase).distinct();
    }

    private Flux<String> splitNameChars(String name) {
        return Flux.fromArray(name.split(""));
    }

    private Flux<String> splitNameCharsWithDelay(String name) {
        return Flux.fromArray(name.split("")).delayElements(Duration.ofMillis(400));
    }

}
