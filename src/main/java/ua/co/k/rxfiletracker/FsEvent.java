package ua.co.k.rxfiletracker;

import java.nio.file.Path;
import java.util.Objects;

/** One change to one file-system path. */
public final class FsEvent {

    public enum Type {
        CREATED,
        DELETED,
        EDITED
    }

    private final Type type;
    private final Path path;

    private FsEvent(Type type, Path path) {
        this.type = type;
        this.path = Objects.requireNonNull(path, "path");
    }

    static FsEvent created(Path path) {
        return new FsEvent(Type.CREATED, path);
    }

    static FsEvent deleted(Path path) {
        return new FsEvent(Type.DELETED, path);
    }

    static FsEvent edited(Path path) {
        return new FsEvent(Type.EDITED, path);
    }

    public Type getType() {
        return type;
    }

    public Path getPath() {
        return path;
    }
}
