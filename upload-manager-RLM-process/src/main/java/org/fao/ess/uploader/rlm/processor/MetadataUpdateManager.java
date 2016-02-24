package org.fao.ess.uploader.rlm.processor;

import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.storage.BinaryStorage;
import org.fao.ess.uploader.core.process.PostUpload;
import org.fao.ess.uploader.core.process.ProcessInfo;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Map;

@ApplicationScoped
@ProcessInfo(context = "c", name = "RLMMetadataUpdate", priority = 2)
public class MetadataUpdateManager implements PostUpload {
    @Inject D3SClient d3sClient;

    @Override
    public void chunkUploaded(ChunkMetadata metadata, BinaryStorage storage) throws Exception {
        //Nothing to do here
    }

    @Override
    public void fileUploaded(FileMetadata metadata, BinaryStorage storage, Map<String, Object> processingParams) throws Exception {
        //Update D3S resources
        d3sClient.sendDataUpdatedSignal();
    }
    
}
