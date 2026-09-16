package ua.co.k.rxfiletracker;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.observers.TestObserver;
import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
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
    public void subscribesAndListensForEvents() throws Exception {
        Path directory = Files.createTempDirectory("rxfiletracker-listen-");
        Path file = directory.resolve("example.txt");
        CountDownLatch eventReceived = new CountDownLatch(1);
        AtomicReference<FsEvent> receivedEvent = new AtomicReference<>();

        Disposable subscription = RxFileTracker.watch(directory, 20L).subscribe(event -> {
            if (event.getPath().equals(file)) {
                receivedEvent.set(event);
                eventReceived.countDown();
            }
        });

        try {
            Files.createFile(file);

            assertTrue(eventReceived.await(2, TimeUnit.SECONDS));
            assertNotNull(receivedEvent.get());
            assertEquals(FsEvent.Type.CREATED, receivedEvent.get().getType());
            assertEquals(file, receivedEvent.get().getPath());
        } finally {
            subscription.dispose();
            Files.deleteIfExists(file);
            Files.deleteIfExists(directory);
        }
    }

    @Test
    public void emitsAbsolutePathsWhenWatchingRelativeDirectory() throws Exception {
        Path directory = Files.createTempDirectory(
                Paths.get("."), "rxfiletracker-relative-");
        Path file = directory.resolve("example.txt");
        Path expectedPath = file.toAbsolutePath().normalize();
        CountDownLatch eventReceived = new CountDownLatch(1);
        AtomicReference<FsEvent> receivedEvent = new AtomicReference<>();

        Disposable subscription = RxFileTracker.watch(directory, 20L).subscribe(event -> {
            if (event.getType() == FsEvent.Type.CREATED
                    && event.getPath().getFileName().equals(file.getFileName())) {
                receivedEvent.set(event);
                eventReceived.countDown();
            }
        });

        try {
            Files.createFile(file);

            assertTrue(eventReceived.await(2, TimeUnit.SECONDS));
            assertTrue(receivedEvent.get().getPath().isAbsolute());
            assertEquals(expectedPath, receivedEvent.get().getPath());
        } finally {
            subscription.dispose();
            Files.deleteIfExists(file);
            Files.deleteIfExists(directory);
        }
    }

    @Test
    public void reportsMissingDirectory() throws Exception {
        Path directory = Files.createTempDirectory("rxfiletracker-missing-");
        Files.delete(directory);

        TestObserver<FsEvent> observer = RxFileTracker.watch(directory, 20L).test();

        observer.assertError(IllegalArgumentException.class);
    }

    @Test
    public void ignoresEventsAfterDisposal() {
        AtomicReference<FileAlterationListener> listener = new AtomicReference<>();
        TestObserver<FsEvent> observer = Observable.<FsEvent>create(emitter ->
                listener.set(new FileAlterationListenerAdaptor() {
                    @Override
                    public void onDirectoryCreate(File directory) {
                        emit(FsEvent.created(directory.toPath()));
                    }

                    @Override
                    public void onDirectoryChange(File directory) {
                        emit(FsEvent.edited(directory.toPath()));
                    }

                    @Override
                    public void onDirectoryDelete(File directory) {
                        emit(FsEvent.deleted(directory.toPath()));
                    }

                    @Override
                    public void onFileCreate(File file) {
                        emit(FsEvent.created(file.toPath()));
                    }

                    @Override
                    public void onFileChange(File file) {
                        emit(FsEvent.edited(file.toPath()));
                    }

                    @Override
                    public void onFileDelete(File file) {
                        emit(FsEvent.deleted(file.toPath()));
                    }

                    private void emit(FsEvent event) {
                        if (!emitter.isDisposed()) {
                            emitter.onNext(event);
                        }
                    }
                })).test();

        observer.dispose();
        listener.get().onFileCreate(new File("ignored.txt"));

        observer.assertNoValues();
    }

    @Test
    public void emitsDirectoryEvents() throws Exception {
        Path root = Files.createTempDirectory("rxfiletracker-directories-");
        Path directory = root.resolve("tracked");
        CountDownLatch created = new CountDownLatch(1);
        CountDownLatch edited = new CountDownLatch(1);
        CountDownLatch deleted = new CountDownLatch(1);
        Disposable subscription = RxFileTracker.watch(root, 20L).subscribe(event -> {
            if (!event.getPath().equals(directory)) {
                return;
            }
            if (event.getType() == FsEvent.Type.CREATED) {
                created.countDown();
            } else if (event.getType() == FsEvent.Type.EDITED) {
                edited.countDown();
            } else if (event.getType() == FsEvent.Type.DELETED) {
                deleted.countDown();
            }
        });

        try {
            Files.createDirectory(directory);
            assertTrue(created.await(2, TimeUnit.SECONDS));

            long lastModified = Files.getLastModifiedTime(directory).toMillis();
            Files.setLastModifiedTime(directory, FileTime.fromMillis(lastModified + 2_000L));
            assertTrue(edited.await(2, TimeUnit.SECONDS));

            Files.delete(directory);
            assertTrue(deleted.await(2, TimeUnit.SECONDS));
        } finally {
            subscription.dispose();
            Files.deleteIfExists(directory);
            Files.deleteIfExists(root);
        }
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
