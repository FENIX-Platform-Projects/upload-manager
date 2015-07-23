package org.fao.ess.uploader.core.dto;

import java.util.*;

public class FileMetadata {

    private String context;
    private String name;
    private String md5;
    private Long size;
    private boolean zip;
    private Integer chunksNumber;
    private Date date;
    private FileStatus status;
    private Map<String,Object> properties;
    private boolean autoClose;



    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMd5() {
        return md5;
    }

    public void setMd5(String md5) {
        this.md5 = md5;
    }

    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }

    public boolean isZip() {
        return zip;
    }

    public void setZip(boolean zip) {
        this.zip = zip;
    }

    public Integer getChunksNumber() {
        return chunksNumber;
    }

    public void setChunksNumber(Integer chunksNumber) {
        this.chunksNumber = chunksNumber;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public FileStatus getStatus() {
        return status;
    }

    public void setStatus(FileStatus status) {
        this.status = status;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public boolean isAutoClose() {
        return autoClose;
    }

    public void setAutoClose(boolean autoClose) {
        this.autoClose = autoClose;
    }
}
