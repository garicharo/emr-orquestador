package com.orquestador;

import java.util.Objects;

public record Task(
        String id,
        String dependsOn,
        WriteMode writeMode,
        String glueDatabase,
        String glueTable) {

    public Task {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(writeMode, "writeMode");
        Objects.requireNonNull(glueDatabase, "glueDatabase");
        Objects.requireNonNull(glueTable, "glueTable");
        if (id.isBlank()) {
            throw new IllegalArgumentException("id is blank");
        }
        if (id.equals(dependsOn)) {
            throw new IllegalArgumentException("task " + id + " depends on itself");
        }
    }
}
