package ua.co.k.rxfiletracker;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import io.reactivex.rxjava3.core.Observable;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;

/** Adapts a Commons IO file monitor to an RxJava observable. */
public final class RxFileAlterationMonitor {

    private final Path directory;
    private final long pollingInterval;

    public RxFileAlterationMonitor(Path directory, long pollingInterval) {
        this.directory = Objects.requireNonNull(directory, "directory");
        if (pollingInterval <= 0) {
            throw new IllegalArgumentException("pollingInterval must be greater than zero");
        }
        this.pollingInterval = pollingInterval;
    }

    public Observable<FsEvent> observe() {
        return Observable.create(emitter -> {
            if (!Files.isDirectory(directory)) {
                emitter.onError(new IllegalArgumentException(
                        "Directory does not exist: " + directory));
                return;
            }

            FileAlterationObserver observer = FileAlterationObserver.builder()
                    .setPath(directory)
                    .get();
            observer.addListener(new FileAlterationListenerAdaptor() {
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
            });

            FileAlterationMonitor monitor =
                    new FileAlterationMonitor(pollingInterval, observer);
            try {
                monitor.start();
                emitter.setCancellable(monitor::stop);
            } catch (Exception error) {
                emitter.onError(error);
            }
        });
    }
}
