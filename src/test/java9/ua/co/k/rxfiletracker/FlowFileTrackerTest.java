package ua.co.k.rxfiletracker;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class FlowFileTrackerTest {

    @Test
    public void usesDefaultPollingInterval() {
        assertNotNull(FlowFileTracker.watch(Paths.get(".")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNonPositivePollingInterval() {
        FlowFileTracker.watch(Paths.get("."), 0L);
    }

    @Test
    public void publishesEventsThroughJavaFlow() throws Exception {
        Path directory = Files.createTempDirectory("flow-file-tracker-");
        Path file = directory.resolve("example.txt");
        CountDownLatch eventReceived = new CountDownLatch(1);
        AtomicReference<FsEvent> receivedEvent = new AtomicReference<>();
        AtomicReference<Flow.Subscription> subscription = new AtomicReference<>();

        FlowFileTracker.watch(directory, 20L).subscribe(new Flow.Subscriber<FsEvent>() {
            @Override
            public void onSubscribe(Flow.Subscription newSubscription) {
                subscription.set(newSubscription);
            }

            @Override
            public void onNext(FsEvent event) {
                if (event.getPath().equals(file)) {
                    receivedEvent.set(event);
                    eventReceived.countDown();
                }
            }

            @Override
            public void onError(Throwable error) {
                throw new AssertionError(error);
            }

            @Override
            public void onComplete() {
            }
        });

        try {
            Files.createFile(file);

            assertFalse(eventReceived.await(200, TimeUnit.MILLISECONDS));
            subscription.get().request(1L);
            assertTrue(eventReceived.await(2, TimeUnit.SECONDS));
            assertEquals(FsEvent.Type.CREATED, receivedEvent.get().getType());
            assertEquals(file, receivedEvent.get().getPath());
        } finally {
            Flow.Subscription activeSubscription = subscription.get();
            if (activeSubscription != null) {
                activeSubscription.cancel();
            }
            Files.deleteIfExists(file);
            Files.deleteIfExists(directory);
        }
    }
}
