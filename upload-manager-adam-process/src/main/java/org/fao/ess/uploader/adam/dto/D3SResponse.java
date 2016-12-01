package org.fao.ess.uploader.adam.dto;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;

@JsonIgnoreProperties(ignoreUnknown = true)
public class D3SResponse {
    @JsonProperty private Integer size;
    @JsonProperty private DSDDataset metadata;

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public DSDDataset getMetadata() {
        return metadata;
    }

    public void setMetadata(DSDDataset metadata) {
        this.metadata = metadata;
    }
}
