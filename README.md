# RxFileAlterationMonitor
RxJava wrapper for Apache's FileAlterationMonitor

Inspirited by [helmbold/rxfilewatcher](https://github.com/helmbold/rxfilewatcher) but utilize org.apache.commons.io.monitor. 

## Java 8 compatibility

On Java 8 and above, subscribe to file-system events with the RxJava API:

```java
Disposable subscription = RxFileTracker.watch(directory)
        .subscribe(event -> System.out.println(event.getPath()));
```

## Java 9+ Flow API

The library is distributed as a multi-release JAR. The existing `RxFileTracker`
API remains compatible with Java 8, while Java 9 and later can use the standard
Flow API:

```java
Flow.Publisher<FsEvent> events = FlowFileTracker.watch(directory);
```
