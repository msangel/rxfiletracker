package ua.co.k.rxfiletracker;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.observers.TestObserver;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RxFileTrackerTest {

    @Test
    public void usesDefaultPollingInterval() {
        assertNotNull(RxFileTracker.watch(Paths.get(".")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNonPositivePollingInterval() {
        RxFileTracker.watch(Paths.get("."), 0L);
    }

    @Test
    public void reportsMissingDirectory() throws Exception {
        Path directory = Files.createTempDirectory("rxfiletracker-missing-");
        Files.delete(directory);

        TestObserver<FsEvent> observer = RxFileTracker.watch(directory, 20L).test();

        observer.assertError(IllegalArgumentException.class);
    }

    @Test
    public void emitsOneTypedEventPerPathChange() throws Exception {
        Path directory = Files.createTempDirectory("rxfiletracker-");
        Path firstFile = directory.resolve("first.txt");
        Path secondFile = directory.resolve("second.txt");
        List<FsEvent> events = new CopyOnWriteArrayList<>();
        CountDownLatch createdFiles = new CountDownLatch(2);
        CountDownLatch editedFile = new CountDownLatch(1);
        CountDownLatch deletedFile = new CountDownLatch(1);
        Disposable subscription = RxFileTracker.watch(directory, 20L).subscribe(event -> {
            events.add(event);
            if (event.getType() == FsEvent.Type.CREATED
                    && (event.getPath().equals(firstFile)
                    || event.getPath().equals(secondFile))) {
                createdFiles.countDown();
            } else if (event.getType() == FsEvent.Type.EDITED
                    && event.getPath().equals(firstFile)) {
                editedFile.countDown();
            } else if (event.getType() == FsEvent.Type.DELETED
                    && event.getPath().equals(secondFile)) {
                deletedFile.countDown();
            }
        });

        try {
            Files.createFile(firstFile);
            Files.createFile(secondFile);

            assertTrue(createdFiles.await(2, TimeUnit.SECONDS));
            Files.write(firstFile, new byte[]{1});
            assertTrue(editedFile.await(2, TimeUnit.SECONDS));
            Files.delete(secondFile);
            assertTrue(deletedFile.await(2, TimeUnit.SECONDS));

            assertTrue(events.stream().anyMatch(
                    event -> event.getType() == FsEvent.Type.CREATED
                            && event.getPath().equals(firstFile)));
            assertTrue(events.stream().anyMatch(
                    event -> event.getType() == FsEvent.Type.CREATED
                            && event.getPath().equals(secondFile)));
        } finally {
            subscription.dispose();
            Files.deleteIfExists(firstFile);
            Files.deleteIfExists(secondFile);
            Files.deleteIfExists(directory);
        }
    }
}
