package org.fao.ess.uploader.gift.bulk.impl;

import org.fao.ess.uploader.core.init.UploaderConfig;
import org.fao.ess.uploader.gift.bulk.dto.Items;
import org.fao.ess.uploader.gift.bulk.dto.MetadataTemplates;
import org.fao.ess.uploader.gift.bulk.utils.D3SClient;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.commons.utils.FileUtils;
import org.fao.fenix.commons.utils.Groups;
import org.fao.fenix.commons.utils.JSONUtils;

import javax.inject.Inject;
import java.io.InputStream;
import java.util.Collection;
import java.util.LinkedList;

public class MetadataManager {

    @Inject private UploaderConfig config;
    @Inject private FileUtils fileUtils;
    @Inject private D3SClient d3SClient;


    public void updateSurveyMetadata(String surveyCode) throws Exception {
        String d3sBaseURL = config.get("gift.d3s.url");
        d3sBaseURL = d3sBaseURL + (d3sBaseURL.charAt(d3sBaseURL.length() - 1) != '/' ? "/" : "");
        d3SClient.updateDatasetMetadataUpdateDate(d3sBaseURL, "gift_"+surveyCode, null);
    }

    public void updateProcessingDatasetsMetadata (String survey) throws Exception {
        String d3sBaseURL = config.get("gift.d3s.url");
        d3sBaseURL = d3sBaseURL + (d3sBaseURL.charAt(d3sBaseURL.length() - 1) != '/' ? "/" : "");

        Collection<MeIdentification<DSDDataset>> newMetadatyaList = createMetadata(survey);
        Collection<MeIdentification<DSDDataset>> existingMetadatyaList = loadExistingMetadata(d3sBaseURL);
        Groups<MeIdentification<DSDDataset>> metadataGroups = new Groups(newMetadatyaList, existingMetadatyaList);

        d3SClient.deleteMetadata(d3sBaseURL, metadataGroups.update);
        d3SClient.insertMetadata(d3sBaseURL, newMetadatyaList);
    }

    private Collection<MeIdentification<DSDDataset>> createMetadata (String survey) throws Exception {
        Collection<MeIdentification<DSDDataset>> metadataInstances = new LinkedList<>();
        for (MetadataTemplates template : MetadataTemplates.values()) {
            MeIdentification<DSDDataset> metadata = loadMetadataTemplate(template);
            String uid = metadata.getUid();
            if (template.bySurvey)
                uid += '_' + survey;
            if (template.byItem)
                for (Items item : Items.values()) {
                    metadata.setUid(uid + '_' + item);
                    metadataInstances.add(metadata);
                }
            else {
                metadata.setUid(uid);
                metadataInstances.add(metadata);
            }
        }
        return metadataInstances;
    }

    private Collection<MeIdentification<DSDDataset>> loadExistingMetadata (String d3sBaseURL) throws Exception {
        return d3SClient.retrieveMetadata(d3sBaseURL, "gift_process");
    }

    //Utils
    private static final String templatePath = "/gift/metadata/templates/";
    private MeIdentification<DSDDataset> loadMetadataTemplate(MetadataTemplates template) throws Exception {
        InputStream templateStream = this.getClass().getResourceAsStream(templatePath+template.toString()+".json");
        if (templateStream==null)
            return null;
        String templateContent = fileUtils.readTextFile(templateStream);
        return JSONUtils.decode(templateContent, MeIdentification.class, DSDDataset.class);
    }
}
