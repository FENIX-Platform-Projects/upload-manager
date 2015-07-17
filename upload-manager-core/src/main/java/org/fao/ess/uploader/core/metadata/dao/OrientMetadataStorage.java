package org.fao.ess.uploader.core.metadata.dao;

import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.metadata.MetadataStorage;

import java.util.Collection;

public class OrientMetadataStorage extends MetadataStorage {

    @Override
    public void init() throws Exception {

    }

    @Override
    public FileMetadata load(String md5) throws Exception {
        return null;
    }

    @Override
    public Collection<ChunkMetadata> load(FileMetadata metadata) throws Exception {
        return null;
    }

    @Override
    public Collection<FileMetadata> select(String context) throws Exception {
        return null;
    }

    @Override
    public void save(FileMetadata metadata) throws Exception {

    }

    @Override
    public void save(ChunkMetadata metadata) throws Exception {

    }

    @Override
    public void remove(String md5) throws Exception {

    }

    @Override
    public void remove(String md5, int index) throws Exception {

    }

    @Override
    public void removeAll(String context) throws Exception {

    }
}
