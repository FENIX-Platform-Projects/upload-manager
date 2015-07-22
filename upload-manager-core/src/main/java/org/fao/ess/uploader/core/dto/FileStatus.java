package org.fao.ess.uploader.core.dto;

import java.util.Set;
import java.util.TreeSet;

public class FileStatus {
    private Long currentSize;
    private Set<Integer> chunksIndex;
    private Boolean complete;


    public Long getCurrentSize() {
        return currentSize;
    }

    public void setCurrentSize(Long currentSize) {
        this.currentSize = currentSize;
    }
    public Set<Integer> getChunksIndex() {
        return chunksIndex;
    }

    public void setChunksIndex(Set<Integer> chunksIndex) {
        this.chunksIndex = chunksIndex != null ? new TreeSet<>(chunksIndex) : null;
    }


    public Boolean isComplete() {
        return complete;
    }

    public void setComplete(Boolean complete) {
        this.complete = complete;
    }
}
