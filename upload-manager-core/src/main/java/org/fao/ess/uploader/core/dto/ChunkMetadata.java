package org.fao.ess.uploader.core.dto;

public class ChunkMetadata {

    private FileMetadata file;
    private int index;
    private Long size;
    private boolean uploaded;


    public FileMetadata getFile() {
        return file;
    }

    public void setFile(FileMetadata file) {
        this.file = file;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }

    public boolean isUploaded() {
        return uploaded;
    }

    public void setUploaded(boolean uploaded) {
        this.uploaded = uploaded;
    }
}
