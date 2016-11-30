package org.fao.ess.uploader.adam.bulk;

import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.metadata.MetadataStorage;
import org.fao.ess.uploader.core.process.PostUpload;
import org.fao.ess.uploader.core.process.ProcessInfo;
import org.fao.ess.uploader.core.storage.BinaryStorage;
import java.util.Map;

@ProcessInfo(context = "adam.bulk", name = "AdamBulk", priority = 1)
public class AdamBulk implements PostUpload {


    @Override
    public void chunkUploaded(ChunkMetadata metadata, MetadataStorage metadataStorage, BinaryStorage storage) throws Exception {

    }

    @Override
    public void fileUploaded(FileMetadata metadata, MetadataStorage metadataStorage, BinaryStorage storage, Map<String, Object> processingParams) throws Exception {

    }
}
