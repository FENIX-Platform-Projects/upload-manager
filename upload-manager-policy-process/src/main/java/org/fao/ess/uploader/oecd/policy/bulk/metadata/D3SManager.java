package org.fao.ess.uploader.oecd.policy.bulk.metadata;

import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.init.UploaderConfig;
import org.fao.ess.uploader.core.metadata.MetadataStorage;
import org.fao.ess.uploader.core.process.PostUpload;
import org.fao.ess.uploader.core.process.ProcessInfo;
import org.fao.ess.uploader.core.storage.BinaryStorage;
import org.fao.ess.uploader.oecd.policy.bulk.metadata.impl.XLStoCSV;
import org.fao.ess.uploader.oecd.policy.bulk.metadata.dto.MetadataGroups;
import org.fao.ess.uploader.oecd.policy.bulk.utils.D3SClient;
import org.fao.ess.uploader.oecd.policy.bulk.metadata.impl.MetadataCreator;
import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.Code;
import org.fao.fenix.commons.msd.dto.full.DSDCodelist;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.commons.utils.FileUtils;

import javax.inject.Inject;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

@ProcessInfo(context = "policy.metadata.bulk", name = "PolicyMetadataBulk", priority = 1)
public class D3SManager implements PostUpload {
    @Inject private XLStoCSV csvConverter;
    @Inject private MetadataCreator metadataFactory;
    @Inject private UploaderConfig config;
    @Inject private D3SClient d3SClient;
    @Inject private FileUtils fileUtils;



    @Override
    public void chunkUploaded(ChunkMetadata metadata, MetadataStorage metadataStorage, BinaryStorage storage) throws Exception {
        //Nothing to do here
    }

    @Override
    public void fileUploaded(FileMetadata metadata, MetadataStorage metadataStorage, BinaryStorage storage, Map<String, Object> processingParams) throws Exception {
        InputStream excelFileInput = storage.readFile(metadata, null);
        Collection<MeIdentification<DSDDataset>> metadataList = createMetadata(excelFileInput, "dsdDefault");
        sendMetadata(metadataList);
    }

    public Collection<MeIdentification<DSDDataset>> createMetadata (InputStream excelFileInput, String dsdTemplate) throws Exception {
        Iterator<String[]> csvIterator = csvConverter.toCSV(excelFileInput).iterator();
        metadataFactory.setHeader(csvIterator.next());
        DSDDataset defaultDSD = metadataFactory.create(dsdTemplate);

        Collection<String> errors = new LinkedList<>();
        Collection<MeIdentification<DSDDataset>> metadataList = new LinkedList<>();
        for (int i=2; csvIterator.hasNext(); i++)
            try {
                MeIdentification<DSDDataset> metadata = metadataFactory.create(csvIterator.next());
                metadata.setDsd(defaultDSD);
                metadataList.add(metadata);
            } catch (Exception ex) {
                errors.add("Errors found for row "+i+":\n"+ex.getMessage()+"\n");
            }

        if (errors.size() > 0) {
            StringBuilder message = new StringBuilder();
            for (String error : errors)
                message.append(error).append('\n');
            throw new Exception(message.toString());
        }

        return metadataList;
    }

    public void sendMetadata (Collection<MeIdentification<DSDDataset>> source) throws Exception {
        //Init
        String baseUrl = config.get("d3s.url");
        baseUrl = baseUrl + (baseUrl.charAt(baseUrl.length() - 1) != '/' ? "/" : "");

        //Load existing metadata
        Collection<MeIdentification<DSDDataset>> destination = d3SClient.retrieveMetadata(baseUrl);

        //Create metadata groups
        MetadataGroups updateGroups = groupMetadata(source, destination);

        //Update metadata
        if (updateGroups.update.size()>0)
            d3SClient.updateMetadata(baseUrl, updateGroups.update);
        if (updateGroups.insert.size()>0)
            d3SClient.insertMetadata(baseUrl, updateGroups.insert);
    }

    public void deleteLegacyMetadata (Collection<String> destinationUid) throws Exception {
        String baseUrl = config.get("d3s.url");
        baseUrl = baseUrl + (baseUrl.charAt(baseUrl.length() - 1) != '/' ? "/" : "");

        if (destinationUid.size()>0) {
            Collection<MeIdentification<DSDDataset>> destination = new LinkedList<>();
            for (String uid : destinationUid) {
                MeIdentification<DSDDataset> metadata = new MeIdentification<>();
                metadata.setUid(uid);
                destination.add(metadata);
            }
            d3SClient.deleteMetadata(baseUrl, destination);
        }

    }

    public void sendLastUpdateDateUpdate (String context) throws Exception {
        //Init
        String baseUrl = config.get("d3s.url");
        baseUrl = baseUrl + (baseUrl.charAt(baseUrl.length() - 1) != '/' ? "/" : "");

        //Send update request
        d3SClient.updateDatasetMetadataUpdateDate(baseUrl, context);
    }

    public void sendCodelists(Collection<Resource<DSDCodelist, Code>> resources) throws Exception {
        //Init
        String baseUrl = config.get("d3s.url");
        baseUrl = baseUrl + (baseUrl.charAt(baseUrl.length() - 1) != '/' ? "/" : "");
        //And send
        if (resources!=null && resources.size()>0)
            d3SClient.updateCodelists(baseUrl,resources);
    }

    private MetadataGroups groupMetadata(Collection<MeIdentification<DSDDataset>> source, Collection<MeIdentification<DSDDataset>> destination) {
        MetadataGroups groups = new MetadataGroups();

        groups.insert.addAll(source);
        groups.insert.removeAll(destination);

        groups.update.addAll(source);
        groups.update.retainAll(destination);

        groups.delete.addAll(destination);
        groups.delete.removeAll(source);

        return groups;
    }



    public static void main(String[] args) {
        try {
            FileInputStream excelFileInput = new FileInputStream("test/Metadatafile_22Apr2016.xlsx");
            D3SManager logic = new D3SManager();
            logic.csvConverter = new XLStoCSV();
            logic.metadataFactory = new MetadataCreator();
            logic.metadataFactory.fileUtils = new FileUtils();
            logic.d3SClient = new D3SClient();
            logic.config = new UploaderConfig();
            logic.config.add("d3s.url", "http://fenix.fao.org/d3s_dev/");

            Collection<MeIdentification<DSDDataset>> metadataList = logic.createMetadata(excelFileInput, "dsdDefault");
            logic.sendMetadata(metadataList);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}




