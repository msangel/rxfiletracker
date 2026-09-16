package ua.co.k.rxfiletracker;

import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.Flow;

import io.reactivex.rxjava3.core.BackpressureStrategy;

/** Java Flow entry point for observing file-system changes on Java 9 and later. */
public final class FlowFileTracker {

    private FlowFileTracker() {
    }

    public static Flow.Publisher<FsEvent> watch(Path directory) {
        return watch(directory, RxFileTracker.DEFAULT_POLLING_INTERVAL);
    }

    public static Flow.Publisher<FsEvent> watch(Path directory, long pollingInterval) {
        org.reactivestreams.Publisher<FsEvent> publisher = RxFileTracker
                .watch(directory, pollingInterval)
                .toFlowable(BackpressureStrategy.BUFFER);
        return subscriber -> {
            Objects.requireNonNull(subscriber, "subscriber");
            //noinspection ReactiveStreamsSubscriberImplementation
            publisher.subscribe(new org.reactivestreams.Subscriber<>() {
                @Override
                public void onSubscribe(org.reactivestreams.Subscription subscription) {
                    subscriber.onSubscribe(new Flow.Subscription() {
                        @Override
                        public void request(long count) {
                            subscription.request(count);
                        }

                        @Override
                        public void cancel() {
                            subscription.cancel();
                        }
                    });
                }

                @Override
                public void onNext(FsEvent event) {
                    subscriber.onNext(event);
                }

                @Override
                public void onError(Throwable error) {
                    subscriber.onError(error);
                }

                @Override
                public void onComplete() {
                    subscriber.onComplete();
                }
            });
        };
    }
}
