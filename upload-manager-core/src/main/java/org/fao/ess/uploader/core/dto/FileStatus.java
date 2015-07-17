package org.fao.ess.uploader.core.dto;

public class FileStatus {
    private Long currentSize;
    private Integer[] chunksIndex;
    private Boolean complete;


    public Long getCurrentSize() {
        return currentSize;
    }

    public void setCurrentSize(Long currentSize) {
        this.currentSize = currentSize;
    }

    public Integer[] getChunksIndex() {
        return chunksIndex;
    }

    public void setChunksIndex(Integer[] chunksIndex) {
        this.chunksIndex = chunksIndex;
    }

    public Boolean isComplete() {
        return complete;
    }

    public void setComplete(Boolean complete) {
        this.complete = complete;
    }
}
