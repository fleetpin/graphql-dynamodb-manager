package com.fleetpin.graphql.database.manager;

public class ParallelStartsWith {
    private String parallelIndex;
    private String index;

    public ParallelStartsWith(String parallelIndex, String index) {
        this.parallelIndex = parallelIndex;
        this.index = index;
    }


    public String getParallelIndex() {
        return parallelIndex;
    }

    public String getIndex() {
        return index;
    }
}
