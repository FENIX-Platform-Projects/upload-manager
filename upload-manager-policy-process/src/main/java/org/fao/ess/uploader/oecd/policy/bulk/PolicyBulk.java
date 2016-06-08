package org.fao.ess.uploader.oecd.policy.bulk;

import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.metadata.MetadataStorage;
import org.fao.ess.uploader.core.process.PostUpload;
import org.fao.ess.uploader.core.process.ProcessInfo;
import org.fao.ess.uploader.core.storage.BinaryStorage;
import org.fao.ess.uploader.oecd.policy.bulk.metadata.MetadataBulk;
import org.fao.ess.uploader.oecd.policy.bulk.attachments.impl.FileManager;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;

import javax.inject.Inject;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;

@ProcessInfo(context = "policy.bulk", name = "PolicyBulk", priority = 1)
public class PolicyBulk implements PostUpload {
    @Inject private MetadataBulk metadataManager;
    @Inject private FileManager tmpFileManager;


    @Override
    public void chunkUploaded(ChunkMetadata metadata, MetadataStorage metadataStorage, BinaryStorage storage) throws Exception {
        //Nothing to do here
    }

    @Override
    public void fileUploaded(FileMetadata metadata, MetadataStorage metadataStorage, BinaryStorage storage, Map<String, Object> processingParams) throws Exception {


        InputStream excelFileInput = storage.readFile(metadata, null);
        Collection<MeIdentification<DSDDataset>> metadataList = metadataManager.createMetadata(excelFileInput, "dsdDefault");
    }


}