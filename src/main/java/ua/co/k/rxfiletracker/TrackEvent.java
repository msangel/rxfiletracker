package ua.co.k.rxfiletracker;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class TrackEvent {
    private final List<Path> created = new ArrayList<>();
    private final List<Path> deleted = new ArrayList<>();
    private final List<Path> edited = new ArrayList<>();

    public List<Path> getCreated() {
        return created;
    }

    public List<Path> getDeleted() {
        return deleted;
    }

    public List<Path> getEdited() {
        return edited;
    }

}
