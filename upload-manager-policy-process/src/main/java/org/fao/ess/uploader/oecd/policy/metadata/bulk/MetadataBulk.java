package org.fao.ess.uploader.oecd.policy.metadata.bulk;

import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.metadata.MetadataStorage;
import org.fao.ess.uploader.core.process.PostUpload;
import org.fao.ess.uploader.core.process.ProcessInfo;
import org.fao.ess.uploader.core.storage.BinaryStorage;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.commons.utils.FileUtils;
import sftp.SftpPropertiesValues;
import sftp.SftpUpload;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

@ProcessInfo(context = "policy.metadata.bulk", name = "PolicyData", priority = 1)
public class MetadataBulk implements PostUpload {
    @Inject private XLStoCSV csvConverter;
    @Inject private MetadataCreator metadataFactory;


    @Override
    public void chunkUploaded(ChunkMetadata metadata, MetadataStorage metadataStorage, BinaryStorage storage) throws Exception {
        //Nothing to do here
    }

    @Override
    public void fileUploaded(FileMetadata metadata, MetadataStorage metadataStorage, BinaryStorage storage, Map<String, Object> processingParams) throws Exception {
        InputStream excelFileInput = storage.readFile(metadata, null);
        Collection<MeIdentification<DSDDataset>> metadataList = createMetadata(excelFileInput, "dsdDefault");
        //TODO update metadata (delete and insert)
    }

    private Collection<MeIdentification<DSDDataset>> createMetadata (InputStream excelFileInput, String dsdTemplate) throws Exception {
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


    public static void main(String[] args) {
        try {
            FileInputStream input = new FileInputStream("test/Metadatafile_22Apr2016.xlsx");
            MetadataBulk logic = new MetadataBulk();
            logic.csvConverter = new XLStoCSV();
            logic.metadataFactory = new MetadataCreator();
            logic.metadataFactory.fileUtils = new FileUtils();

            logic.createMetadata(input, "dsdDefault");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
