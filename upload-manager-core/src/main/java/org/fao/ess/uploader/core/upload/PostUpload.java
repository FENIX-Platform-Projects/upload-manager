package org.fao.ess.uploader.core.upload;

import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.storage.BinaryStorage;

public interface PostUpload {

    void chunkUploaded(ChunkMetadata metadata, BinaryStorage storage) throws Exception;
    void fileUploaded(FileMetadata metadata, BinaryStorage storage) throws Exception;
}
