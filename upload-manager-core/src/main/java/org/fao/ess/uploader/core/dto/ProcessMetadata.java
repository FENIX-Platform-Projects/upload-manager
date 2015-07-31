package org.fao.ess.uploader.core.dto;

import org.fao.ess.uploader.core.process.PostUpload;

public class ProcessMetadata {
    private String name;
    private boolean completed;
    private int index;
    private String error;
    private FileMetadata file;

    private PostUpload processInstance;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isCompleted() {
        return completed;
    }

    public void setCompleted(boolean completed) {
        this.completed = completed;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public FileMetadata getFile() {
        return file;
    }

    public void setFile(FileMetadata file) {
        this.file = file;
    }

    //Utils
    public PostUpload instance() {
        return processInstance;
    }
    public void instance(PostUpload processInstance) {
        this.processInstance = processInstance;
    }
}
