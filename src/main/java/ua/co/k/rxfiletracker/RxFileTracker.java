package ua.co.k.rxfiletracker;

import java.nio.file.Path;

import io.reactivex.rxjava3.core.Observable;

/** Entry point for observing file-system changes. */
public final class RxFileTracker {

    public static final long DEFAULT_POLLING_INTERVAL = 1_000L;

    private RxFileTracker() {
    }

    public static Observable<FsEvent> watch(Path directory) {
        return watch(directory, DEFAULT_POLLING_INTERVAL);
    }

    public static Observable<FsEvent> watch(Path directory, long pollingInterval) {
        return new RxFileAlterationMonitor(directory, pollingInterval).observe();
    }
}
