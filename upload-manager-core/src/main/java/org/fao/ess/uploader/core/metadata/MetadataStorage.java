package org.fao.ess.uploader.core.metadata;

import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;


public abstract class MetadataStorage {

    public abstract void init() throws Exception;


    public abstract FileMetadata load(String md5) throws Exception;
    public abstract Collection<ChunkMetadata> load(FileMetadata metadata) throws Exception;
    public abstract Collection<FileMetadata> select(String context) throws Exception;

    public abstract void save(FileMetadata metadata) throws Exception;
    public abstract void save(ChunkMetadata metadata) throws Exception;

    public abstract void remove(String md5) throws Exception;
    public abstract void remove(String md5, int index) throws Exception;
    public abstract void removeAll(String context) throws Exception;

}
