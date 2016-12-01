package org.fao.ess.uploader.adam.impl;

import org.fao.ess.uploader.adam.dto.D3SResponse;
import org.fao.ess.uploader.adam.utils.connection.D3SClient;
import org.fao.ess.uploader.core.init.UploaderConfig;

import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.commons.msd.dto.full.MeMaintenance;
import org.fao.fenix.commons.msd.dto.full.SeUpdate;
import org.fao.fenix.commons.utils.FileUtils;

import javax.inject.Inject;
import java.util.Date;

public class MetadataManager {

    @Inject private UploaderConfig config;
    @Inject private FileUtils fileUtils;
    @Inject private D3SClient d3SClient;
    private static final String ADAM_CONFIG_URL = "adam.d3s.url";


    public void updateMetadata(String uid) throws Exception {
        String d3sBaseURL = config.get(ADAM_CONFIG_URL);
        d3sBaseURL = d3sBaseURL + (d3sBaseURL.charAt(d3sBaseURL.length() - 1) != '/' ? "/" : "");

        //Create metadata bean
        MeIdentification<DSDDataset> metadata = new MeIdentification<>();
        metadata.setUid(uid);
        MeMaintenance meMaintenance = new MeMaintenance();
        metadata.setMeMaintenance(meMaintenance);
        SeUpdate seUpdate = new SeUpdate();
        seUpdate.setUpdateDate(new Date());
        meMaintenance.setSeUpdate(seUpdate);

        d3SClient.appendDatasetMetadata(d3sBaseURL, metadata);
    }

    public void updateCache(String uid) throws Exception {
        String d3sBaseURL = config.get(ADAM_CONFIG_URL);
        D3SResponse response = d3SClient.getDataset(d3sBaseURL,uid,null,10,1);
        while (response.getSize()%10 ==0)
            response = d3SClient.getDataset(d3sBaseURL,uid,null,10,1);
    }



}
