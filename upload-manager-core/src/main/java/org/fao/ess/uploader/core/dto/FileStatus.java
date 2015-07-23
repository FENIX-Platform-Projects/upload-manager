package org.fao.ess.uploader.core.dto;

import java.util.Set;
import java.util.TreeSet;

public class FileStatus {
    private Long currentSize;
    private Set<Integer> chunksIndex;
    private Boolean complete;
    private String error;


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

    public Boolean getComplete() {
        return complete;
    }

    public void setComplete(Boolean complete) {
        this.complete = complete;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }


    //Utils
    public void addChunkIndex(Integer index) {
        if (index!=null) {
            if (chunksIndex==null)
                chunksIndex = new TreeSet<>();
            chunksIndex.add(index);
        }
    }
}
