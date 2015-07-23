package org.fao.ess.uploader.core.upload;

import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.dto.FileStatus;
import org.fao.ess.uploader.core.metadata.MetadataStorage;
import org.fao.ess.uploader.core.metadata.MetadataStorageFactory;
import org.fao.ess.uploader.core.storage.BinaryStorage;
import org.fao.ess.uploader.core.storage.BinaryStorageFactory;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@Path("file")
public class FileService {
    @Inject MetadataStorageFactory metadataFactory;
    @Inject BinaryStorageFactory binaryFactory;
    @Inject UploadFactory processorsFactory;


    @GET
    @Path("{context}/{md5}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response getFile(@PathParam("context") String context, @PathParam("md5") String md5) throws Exception {
        MetadataStorage metadataStorage = metadataFactory.getInstance();
        BinaryStorage binaryStorage = binaryFactory.getInstance();
        //Load file metadata and check file availability
        FileMetadata fileMetadata = metadataStorage.load(context, md5);
        if (fileMetadata==null)
            throw new NotFoundException("File not found\ncontext: "+context+" - md5: "+md5);
        if (!fileMetadata.getStatus().getComplete())
            throw new NotAllowedException("File incomplete");
        //Create file stream
        final InputStream data = binaryStorage.readFile(fileMetadata,null);
        StreamingOutput stream = new StreamingOutput() {
            public void write(OutputStream output) throws IOException, WebApplicationException {
                try {
                    byte[] buffer = new byte[1024];
                    for (int c = data.read(buffer); c>0; c = data.read(buffer))
                        output.write(buffer, 0, c);
                } catch (Exception e) {
                    throw new WebApplicationException(e);
                }
            }
        };
        //Build response
        Response.ResponseBuilder response = Response.ok(stream, MediaType.APPLICATION_OCTET_STREAM);
        if (fileMetadata.getName()!=null)
            response = response.header("content-disposition", "attachment; filename=\""+fileMetadata.getName()+"\"");
        else
            response = response.header("content-disposition", "attachment");
        if (fileMetadata.getSize()!=null)
            response = response.header("Content-Length", fileMetadata.getSize());
        //Return response
        return response.build();
    }


    @POST
    @Path("{context}/{md5}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public void uploadFile (@Context HttpServletRequest request, @PathParam("context") String context, @PathParam("md5") String md5) throws Exception {
        MetadataStorage metadataStorage = metadataFactory.getInstance();
        //Remove existing file if not completed
        FileMetadata metadata = metadataStorage.load(context,md5);
        if (metadata!=null) {
            if (metadata.getStatus().getComplete())
                throw new NotAllowedException("File already uploaded");
            else
                removeFile(context, md5);
        }
        //Prepare an auto-close file metadata
        metadata = new FileMetadata();
        metadata.setContext(context);
        metadata.setMd5(md5);
        metadata.setAutoClose(true);
        metadata.setChunksNumber(1);
        //Save metadata
        metadataStorage.save(metadata);
        //Save chunk and close file
        uploadChunk(request, context, md5, 0);
    }

    @POST
    @Path("{context}/{md5}/{index}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public void uploadChunk (@Context HttpServletRequest request, @PathParam("context") String context, @PathParam("md5") String md5, @PathParam("index") Integer index) throws Exception {
        MetadataStorage metadataStorage = metadataFactory.getInstance();
        BinaryStorage binaryStorage = binaryFactory.getInstance();
        //Check if chunk already exists
        ChunkMetadata metadata = metadataStorage.load(context, md5, index);
        if (metadata!=null) {
            if (metadata.isUploaded())
                throw new NotAllowedException("");
            else
                binaryStorage.removeChunk(metadata);
        }
        //Prepare chunk metadata
        FileMetadata fileMetadata = metadataStorage.load(context,md5);
        if (fileMetadata==null)
            throw new NotFoundException("File not found\ncontext: "+context+" - md5: "+md5);
        FileStatus status = fileMetadata.getStatus();
        metadata = new ChunkMetadata();
        metadata.setIndex(index);
        metadata.setFile(fileMetadata);
        //Save metadata
        metadataStorage.save(metadata);
        //Pre process data
        InputStream processedData, data = request.getInputStream();
        for (PreUpload preUpload : processorsFactory.getPreUploadInstances(context))
            if ((processedData = preUpload.uploadingChunk(metadata,data)) != null)
                data = processedData;
        //Save data
        binaryStorage.writeChunk(metadata, data);
        //Update chunk metadata
        metadata.setUploaded(true);
        metadataStorage.save(metadata);
        //Update file metadata
        status.addChunkIndex(index);
        metadataStorage.save(fileMetadata);
        //Post process chunk data
        for (PostUpload postUpload : processorsFactory.getPostUploadInstances(context))
            postUpload.chunkUploaded(metadata,binaryStorage);
        //Close file automatically if required
        if (fileMetadata.isAutoClose())
            close(context,md5);
    }


    @POST
    @Path("closure/{context}/{md5}")
    public void close(@PathParam("context") String context, @PathParam("md5") String md5) throws Exception {
        MetadataStorage metadataStorage = metadataFactory.getInstance();
        BinaryStorage binaryStorage = binaryFactory.getInstance();
        //Load file metadata
        FileMetadata fileMetadata = metadataStorage.load(context, md5);
        if (fileMetadata==null)
            throw new NotFoundException("File not found\ncontext: "+context+" - md5: "+md5);
        FileStatus status = fileMetadata.getStatus();
        //Check if file can be closed
        if (fileMetadata.getChunksNumber()==null && status.getChunksIndex()==null && status.getChunksIndex().size()!=fileMetadata.getChunksNumber())
            throw new NotAllowedException("File incomplete");
        //Close file
        binaryStorage.closeFile(fileMetadata);
        //Update file metadata
        status.setComplete(true);
        metadataStorage.save(fileMetadata);
        //Remove chunks
        for (ChunkMetadata chunkMetadata : metadataStorage.loadChunks(context, md5)) {
            binaryStorage.removeChunk(chunkMetadata);
            metadataStorage.remove(context, md5, chunkMetadata.getIndex());
        }
        //Post process uploaded file
        for (PostUpload postUpload : processorsFactory.getPostUploadInstances(context))
            postUpload.fileUploaded(fileMetadata, binaryStorage);
    }


    @DELETE
    @Path("{context}/{md5}")
    public void removeFile (@PathParam("context") String context, @PathParam("md5") String md5) throws Exception {
        MetadataStorage metadataStorage = metadataFactory.getInstance();
        BinaryStorage binaryStorage = binaryFactory.getInstance();
        //Load file metadata and check file availability
        FileMetadata fileMetadata = metadataStorage.load(context, md5);
        if (fileMetadata==null)
            throw new NotFoundException("File not found\ncontext: "+context+" - md5: "+md5);
        //Remove chunks
        for (ChunkMetadata chunkMetadata : metadataStorage.loadChunks(context, md5)) {
            binaryStorage.removeChunk(chunkMetadata);
            metadataStorage.remove(context, md5, chunkMetadata.getIndex());
        }
        //Remove file
        if (fileMetadata.getStatus().getComplete())
            binaryStorage.removeFile(fileMetadata);
        metadataStorage.remove(context, md5);
    }

}
