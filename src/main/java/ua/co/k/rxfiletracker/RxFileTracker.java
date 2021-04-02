package ua.co.k.rxfiletracker;

import java.io.File;
import java.io.IOException;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.reactivex.rxjava3.core.ObservableOnSubscribe;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.functions.Action;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;

public class RxFileTracker {

    public static final String FOLDER =
            "/home/msangel/Desktop/simple-test-monitor/";

    public static void main(String[] args) throws Exception {
        // The monitor will perform polling on the folder every 5 seconds
        final long pollingInterval = 5 * 1000;

        File folder = new File(FOLDER);

        if (!folder.exists()) {
            // Test to see if monitored folder exists
            throw new RuntimeException("Directory not found: " + FOLDER);
        }

        FileAlterationObserver observer = new FileAlterationObserver(folder);
        FileAlterationMonitor monitor =
                new FileAlterationMonitor(pollingInterval);
        observer.addListener(new FileAlterationListener(){
            @Override
            public void onStart(FileAlterationObserver observer) {
                System.out.println("onStart");
            }

            @Override
            public void onDirectoryCreate(File directory) {
                System.out.println("onDirectoryCreate: " + directory);
            }

            @Override
            public void onDirectoryChange(File directory) {
                System.out.println("onDirectoryChange: " + directory);
            }

            @Override
            public void onDirectoryDelete(File directory) {
                System.out.println("onDirectoryChange: " + directory);
            }

            @Override
            public void onFileCreate(File file) {
                System.out.println("onFileCreate: " + file);
            }

            @Override
            public void onFileChange(File file) {
                System.out.println("onFileChange: " + file);
            }

            @Override
            public void onFileDelete(File file) {
                System.out.println("onFileDelete: " + file);
            }

            @Override
            public void onStop(FileAlterationObserver observer) {
                System.out.println("onStop: " + observer);
            }
        });
        monitor.addObserver(observer);
        monitor.start();
        monitor.stop();
    }

    Observable<TrackEvent> create() {
        return Observable.create(new ObservableOnSubscribe<TrackEvent>() {
            @Override
            public void subscribe(@NonNull ObservableEmitter<TrackEvent> emitter) throws Throwable {

                Schedulers.newThread().createWorker().schedule(() -> {

                });
            }
        });
    }

    public void addTarget(){

    }
}
