# RxFileTracker

[![Java 8+](https://img.shields.io/badge/Java-8%2B-007396?logo=openjdk)](https://openjdk.org/)
[![RxJava 3](https://img.shields.io/badge/RxJava-3-B7178C)](https://github.com/ReactiveX/RxJava)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**Reliable, polling-based directory monitoring for Java, exposed as RxJava 3
observables and Java 9+ Flow publishers.**

RxFileTracker watches a directory tree and emits typed `CREATED`, `EDITED`, and
`DELETED` events for files and directories. It is an alternative to
`java.nio.file.WatchService`-based watchers when portable state detection and a
predictable event stream matter more than the lowest possible latency.

```java
Disposable subscription = RxFileTracker.watch(Paths.get("/data/inbox"))
        .subscribe(event -> System.out.printf(
                "%s: %s%n", event.getType(), event.getPath()));

// Stop the underlying monitor when it is no longer needed.
subscription.dispose();
```

## Why RxFileTracker?

- **Portable polling instead of native watch events.** It uses
  [Apache Commons IO](https://commons.apache.org/proper/commons-io/apidocs/org/apache/commons/io/monitor/FileAlterationObserver.html)
  to compare directory state at a configurable interval, so it does not depend
  on a file-system provider offering consistent native notifications.
- **Less event noise.** Multiple low-level modification notifications within
  one polling window are represented by the state observed during that scan,
  instead of being forwarded as a burst of platform-specific events.
- **A small, typed API.** Every event contains an `FsEvent.Type` and a
  `java.nio.file.Path`.
- **Two reactive APIs in one multi-release JAR.** Use RxJava 3 on Java 8+, or
  the standard `java.util.concurrent.Flow` API on Java 9+.
- **Lifecycle-aware.** Disposing an RxJava subscription or cancelling a Flow
  subscription stops its underlying monitor.
- **Verified behavior.** The test suite covers file and directory creation,
  editing, deletion, subscription disposal, Flow demand, invalid paths, and
  polling-interval validation. The Maven build enforces at least 80% line and
  branch coverage.

## When polling is safer than `WatchService`

This project was inspired by
[helmbold/rxfilewatcher](https://github.com/helmbold/rxfilewatcher), which wraps
the JDK `WatchService`. RxFileTracker deliberately uses a different mechanism.

| Concern | RxFileTracker | `rxfilewatcher` / JDK `WatchService` |
| --- | --- | --- |
| Change detection | Compares directory state on every polling cycle | Consumes notifications from the file-system watch provider |
| Lost events under load | Has no native event queue that can overflow; the next scan reconciles the observable state | A bounded implementation may discard events and report `OVERFLOW` |
| File-system support | Works when Java can list the directory and read its file metadata | Detection, ordering, timing, and remote file-system behavior are provider-specific |
| Repeated modify events | At most one observed change per path in a scan cycle | One logical write may produce one event on one platform and several on another |
| Latency and I/O | Detection is delayed by up to roughly one polling interval and each scan costs I/O | Usually lower-latency and more efficient for large, quiet local directory trees |

These are documented limitations of the JDK API: events can be discarded,
modify notifications can be duplicated, and detection on remote storage is not
guaranteed. See the
[`WatchService` platform-dependencies documentation](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/file/WatchService.html#platform.dependencies)
and the definition of
[`OVERFLOW`](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/file/StandardWatchEventKinds.html#OVERFLOW).

Polling is not an absolute delivery guarantee. A file created and deleted
entirely between two scans may not be observed. Choose a shorter interval when
this matters, accepting the additional I/O cost. Choose a `WatchService`-based
library when near-real-time response and minimal scanning overhead are more
important than cross-provider consistency.

## Installation

Until a release is published to Maven Central, install the current version from
source. Building the multi-release JAR requires Maven and JDK 9 or newer:

```bash
git clone https://github.com/msangel/rxfiletracker.git
cd rxfiletracker
mvn clean install
```

Then add the locally installed artifact to your project:

```xml
<dependency>
    <groupId>ua.co.k</groupId>
    <artifactId>rxfiletracker</artifactId>
    <version>1.0.4-SNAPSHOT</version>
</dependency>
```

## RxJava 3 API (Java 8+)

`watch(directory)` recursively monitors the directory with the default
one-second polling interval. Pass a positive interval in milliseconds to tune
the latency/I/O trade-off:

```java
import io.reactivex.rxjava3.disposables.Disposable;
import java.nio.file.Path;
import java.nio.file.Paths;
import ua.co.k.rxfiletracker.FsEvent;
import ua.co.k.rxfiletracker.RxFileTracker;

Path directory = Paths.get("/data/inbox");

Disposable subscription = RxFileTracker.watch(directory, 500L)
        .filter(event -> event.getType() == FsEvent.Type.CREATED)
        .subscribe(
                event -> importFile(event.getPath()),
                error -> log.error("Directory monitor failed", error));
```

Each subscription owns a monitor. Keep the returned `Disposable` and dispose it
during application shutdown.

## Java Flow API (Java 9+)

The same JAR exposes `FlowFileTracker` on Java 9 and later:

```java
import java.nio.file.Paths;
import java.util.concurrent.Flow;
import ua.co.k.rxfiletracker.FlowFileTracker;
import ua.co.k.rxfiletracker.FsEvent;

Flow.Publisher<FsEvent> events =
        FlowFileTracker.watch(Paths.get("/data/inbox"), 500L);

events.subscribe(new Flow.Subscriber<FsEvent>() {
    private Flow.Subscription subscription;

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(FsEvent event) {
        System.out.printf("%s: %s%n", event.getType(), event.getPath());
        subscription.request(1);
    }

    @Override
    public void onError(Throwable error) {
        error.printStackTrace();
    }

    @Override
    public void onComplete() {
    }
});
```

Flow subscribers receive events only after requesting demand. Cancel the
`Flow.Subscription` to stop monitoring.

## Event model

| Type | Meaning |
| --- | --- |
| `CREATED` | A file or directory appeared since the previous scan |
| `EDITED` | A file or directory's observed metadata changed |
| `DELETED` | A file or directory disappeared since the previous scan |

`FsEvent#getPath()` returns the affected absolute, normalized path, even when
the watched directory was supplied as a relative path. The watched path must
exist and be a directory when the stream is subscribed; otherwise the stream
terminates with an `IllegalArgumentException`.

## Common use cases

RxFileTracker is suitable for Java applications that need recursive folder
watching, file arrival detection, hot-folder processing, upload or import
pipelines, configuration reloads, cache invalidation, ETL inbox monitoring,
directory synchronization, audit workflows, or reactive file-system events on
local, mounted, container, and remote storage where native notifications may be
unavailable or inconsistent.

Related topics: Java file watcher, Java directory monitor, RxJava file events,
reactive file monitoring, polling file watcher, `WatchService` alternative,
Apache Commons IO `FileAlterationMonitor`, Java Flow publisher, NIO file-system
events, cross-platform folder watcher.

## Build and verification

```bash
mvn clean verify
```

This runs the tests and JaCoCo coverage checks. The project targets Java 8 for
the core and RxJava API, and packages the Java Flow adapter as a Java 9
multi-release JAR entry.

## License

Licensed under the [Apache License 2.0](LICENSE).
