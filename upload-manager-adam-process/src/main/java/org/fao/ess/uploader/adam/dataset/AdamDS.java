package org.fao.ess.uploader.adam.dataset;

import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.metadata.MetadataStorage;
import org.fao.ess.uploader.core.process.PostUpload;
import org.fao.ess.uploader.core.process.ProcessInfo;
import org.fao.ess.uploader.core.storage.BinaryStorage;
import java.util.Map;

@ProcessInfo(context = "adam.dataset", name = "AdamDS", priority = 1)
public class AdamDS implements PostUpload {


    @Override
    public void chunkUploaded(ChunkMetadata metadata, MetadataStorage metadataStorage, BinaryStorage storage) throws Exception {

    }

    @Override
    public void fileUploaded(FileMetadata metadata, MetadataStorage metadataStorage, BinaryStorage storage, Map<String, Object> processingParams) throws Exception {

    }
}
