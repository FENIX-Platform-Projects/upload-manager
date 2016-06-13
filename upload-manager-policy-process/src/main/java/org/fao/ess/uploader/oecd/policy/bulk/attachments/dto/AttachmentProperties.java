package org.fao.ess.uploader.oecd.policy.bulk.attachments.dto;

public class AttachmentProperties {
    private Integer policyId;
    private String fileName;
    private String md5;
    private Long size;


    public AttachmentProperties() {
    }
    public AttachmentProperties(Integer policyId, String fileName, String md5, Long size) {
        this.policyId = policyId;
        this.fileName = fileName;
        this.md5 = md5;
        this.size = size;
    }


    public Integer getPolicyId() {
        return policyId;
    }

    public void setPolicyId(Integer policyId) {
        this.policyId = policyId;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
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
}
