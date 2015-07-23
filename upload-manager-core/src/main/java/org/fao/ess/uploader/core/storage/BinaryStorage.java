package org.fao.ess.uploader.core.storage;

import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.dto.FileStatus;

import java.io.InputStream;
import java.io.OutputStream;


public abstract class BinaryStorage {

    public abstract void init() throws Exception;
    public abstract void writeChunk(ChunkMetadata chunkMetadata, InputStream input) throws Exception;

    public abstract InputStream readFile(FileMetadata fileMetadata, OutputStream outputStream) throws Exception;

    public abstract void closeFile(FileMetadata fileMetadata) throws Exception;
    public abstract void removeFile(FileMetadata fileMetadata) throws Exception;
    public abstract void removeChunk(ChunkMetadata chunkMetadata) throws Exception;

}
