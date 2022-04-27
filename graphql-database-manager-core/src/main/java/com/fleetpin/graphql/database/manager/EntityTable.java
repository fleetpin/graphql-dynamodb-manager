package com.fleetpin.graphql.database.manager;

import java.util.Optional;

public class EntityTable {
    String name;
    Optional<String> parallelIndex;

    public EntityTable(String name, Optional<String> parallelIndex) {
        this.name = name;
        this.parallelIndex = parallelIndex;
    }

    public String getName() {
        return name;
    }

    public Optional<String> getParallelIndex() {
        return parallelIndex;
    }
}
