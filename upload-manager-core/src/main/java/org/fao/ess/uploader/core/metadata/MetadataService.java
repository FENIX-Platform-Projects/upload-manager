package org.fao.ess.uploader.core.metadata;

import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.dto.FileStatus;

import javax.inject.Inject;
import javax.websocket.server.PathParam;
import javax.ws.rs.*;
import javax.ws.rs.core.NoContentException;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;

@Path("metadata")
public class MetadataService {
    @Inject private MetadataStorageFactory storageFactory;

    //FILE METADATA STORAGE
    @GET
    @Path("file/{context}/{md5}")
    public FileMetadata getFileMetadata(@PathParam("context") String context, @PathParam("md5") String md5) throws Exception {
        MetadataStorage storage = storageFactory.getInstance();
        FileMetadata metadata = storage.load(context, md5);
        if (metadata == null)
            throw new javax.ws.rs.NotFoundException("File not found");
        return metadata;
    }
    @GET
    @Path("file/{context}")
    public Collection<FileMetadata> selectFileMetadata(@PathParam("context") String context) throws Exception {
        MetadataStorage storage = storageFactory.getInstance();
        Collection<FileMetadata> metadataList = storage.select(context);
        if (metadataList.size() == 0)
            throw new javax.ws.rs.NotFoundException("File not found");
        return metadataList;
    }

    @POST
    @Path("file")
    public void newFileMetadata(FileMetadata metadata) throws Exception {
        validate(metadata);
        MetadataStorage storage = storageFactory.getInstance();

        //Check for existing file
        if (storage.load(metadata.getContext(), metadata.getMd5()) != null)
            throw new javax.ws.rs.NotAllowedException("File already exists");

        //Store metadata
        storage.save(metadata);
    }
    @PUT
    @Path("file")
    public void updateFileMetadata(FileMetadata metadata) throws Exception {
        validate(metadata);
        MetadataStorage storage = storageFactory.getInstance();

        //Check for existing file
        if (storage.load(metadata.getContext(), metadata.getMd5()) == null)
            throw new javax.ws.rs.NotFoundException("File not found");

        //Store metadata
        storage.save(metadata);
    }

    @DELETE
    @Path("file/{context}/{md5}")
    public void removeFileMetadata(@PathParam("context") String context, @PathParam("md5") String md5) throws Exception {
        if (!storageFactory.getInstance().remove(context, md5))
            throw new javax.ws.rs.NotFoundException("File not found");
    }


    //CHUNK METADATA STORAGE
    @GET
    @Path("chunk/{context}/{md5}")
    public Collection<ChunkMetadata> selectChunkMetadata(@PathParam("context") String context, @PathParam("md5") String md5) throws Exception {
        MetadataStorage storage = storageFactory.getInstance();
        Collection<ChunkMetadata> metadataList = storage.loadChunks(context,md5);
        if (metadataList.size() == 0)
            throw new javax.ws.rs.NotFoundException("No chunks found");
        return metadataList;
    }

    @DELETE
    @Path("chunk/{context}/{md5}/{index}")
    public void removeChunkMetadata(@PathParam("context") String context, @PathParam("md5") String md5, @PathParam("index") Integer index) throws Exception {
        if (!storageFactory.getInstance().remove(context, md5, index))
            throw new javax.ws.rs.NotFoundException("File not found");
    }

    //UTILS

    private void validate(FileMetadata metadata) throws Exception {
        //Validate metadata
        if (metadata.getContext()==null || metadata.getMd5()==null)
            throw new NoContentException("Metadata identification absent. Context and md5 fields are mandatory");
        //Set default values
        if (metadata.getDate()==null)
            metadata.setDate(new Date());
        if (metadata.getStatus()==null) {
            FileStatus status = new FileStatus();
            status.setComplete(false);
            status.setCurrentSize(0l);
            status.setChunksIndex(new HashSet<Integer>());
            metadata.setStatus(status);
        }
    }

}
