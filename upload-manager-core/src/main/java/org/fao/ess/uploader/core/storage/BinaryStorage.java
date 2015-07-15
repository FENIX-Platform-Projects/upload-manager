package org.fao.ess.uploader.core.storage;

import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.dto.Status;

import java.io.InputStream;


public abstract class BinaryStorage {

    public abstract void init() throws Exception;
    public abstract Status writeChunk (ChunkMetadata chunkMetadata, InputStream input) throws Exception;
    public abstract Status closeFile(FileMetadata fileMetadata) throws Exception;

    public abstract Status checkStatus(String fileName) throws Exception;

}
