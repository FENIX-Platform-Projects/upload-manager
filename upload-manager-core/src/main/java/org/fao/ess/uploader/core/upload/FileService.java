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
import javax.ws.rs.core.*;
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
            throw new NoContentException("File not found\ncontext: "+context+" - md5: "+md5);
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
    public Response uploadFile (@Context HttpServletRequest request, @PathParam("context") String context, @PathParam("md5") String md5) throws Exception {
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

        return Response.created(null).build();
    }

    @POST
    @Path("chunk/{context}/{md5}")
    //@Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Response uploadChunk (@Context HttpServletRequest request, @PathParam("context") String context, @PathParam("md5") String md5, @QueryParam("index") Integer index) throws Exception {
        MetadataStorage metadataStorage = metadataFactory.getInstance();
        BinaryStorage binaryStorage = binaryFactory.getInstance();
        //Load file metadata
        FileMetadata fileMetadata = metadataStorage.load(context, md5);
        if (fileMetadata==null)
            throw new NoContentException("File not found\ncontext: "+context+" - md5: "+md5);
        //Calculate index
        Long size = null;
        Long fileSize = fileMetadata.getSize();
        Integer chunksNumber = fileMetadata.getChunksNumber();
        if (index==null) {
            String positionHeader = request.getHeader("Content-Range");
            Integer bytesWordIndex = positionHeader != null ? positionHeader.indexOf("bytes") : null;
            String positionString = bytesWordIndex != null && bytesWordIndex >= 0 ? positionHeader.substring(bytesWordIndex + "bytes".length()).trim() : null;
            if (positionString != null) {
                long from = Long.parseLong(positionString.substring(0, positionString.indexOf('-')));
                long to = Long.parseLong(positionString.substring(positionString.indexOf('-') + 1, positionString.indexOf('/')));
                size = to-from+1;
                if (fileSize==null)
                    fileMetadata.setSize(fileSize = Long.parseLong(positionString.substring(positionString.indexOf('/') + 1)));
                if (chunksNumber==null)
                    fileMetadata.setChunksNumber(chunksNumber = (int)(fileSize/size + (fileSize%size>0 ? 1 : 0)));
                index = to<(fileSize-1) ? (int)(from/size) : chunksNumber-1;
            }
        }
        //Validate
        if (index==null)
            throw new NotAllowedException("Chunk index is mandatory");
        //Check if chunk already exists
        ChunkMetadata metadata = metadataStorage.load(context, md5, index);
        if (metadata!=null) {
            if (metadata.isUploaded())
                throw new NotAllowedException("");
            else
                binaryStorage.removeChunk(metadata);
        }
        //Prepare chunk metadata
        FileStatus status = fileMetadata.getStatus();
        metadata = new ChunkMetadata();
        metadata.setIndex(index);
        metadata.setFile(fileMetadata);
        if (size!=null)
            metadata.setSize(size);
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
        if (size!=null)
            status.setCurrentSize(status.getCurrentSize() + size);
        metadataStorage.save(fileMetadata);
        //Post process chunk data
        for (PostUpload postUpload : processorsFactory.getPostUploadInstances(context))
            postUpload.chunkUploaded(metadata,binaryStorage);
        //Close file automatically if required
        if (fileMetadata.isAutoClose() && fileMetadata.getChunksNumber()!=null && status.getChunksIndex()!=null && status.getChunksIndex().size()==fileMetadata.getChunksNumber())
            close(context,md5,true);

        return Response.ok().build();
    }


    @POST
    @Path("closure/{context}/{md5}")
    public Response close(@PathParam("context") String context, @PathParam("md5") String md5, @QueryParam("process") @DefaultValue("true") boolean process) throws Exception {
        MetadataStorage metadataStorage = metadataFactory.getInstance();
        BinaryStorage binaryStorage = binaryFactory.getInstance();
        //Load file metadata
        FileMetadata fileMetadata = metadataStorage.load(context, md5);
        if (fileMetadata==null)
            throw new NoContentException("File not found\ncontext: "+context+" - md5: "+md5);
        FileStatus status = fileMetadata.getStatus();
        //Check if file can be closed
        if (fileMetadata.getChunksNumber()==null && (status.getChunksIndex()==null || status.getChunksIndex().size()!=fileMetadata.getChunksNumber()))
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
        if (process)
            for (PostUpload postUpload : processorsFactory.getPostUploadInstances(context))
                postUpload.fileUploaded(fileMetadata, binaryStorage);

        return Response.ok().build();
    }


    @DELETE
    @Path("{context}/{md5}")
    public Response removeFile (@PathParam("context") String context, @PathParam("md5") String md5) throws Exception {
        MetadataStorage metadataStorage = metadataFactory.getInstance();
        BinaryStorage binaryStorage = binaryFactory.getInstance();
        //Load file metadata and check file availability
        FileMetadata fileMetadata = metadataStorage.load(context, md5);
        if (fileMetadata==null)
            throw new NoContentException("File not found\ncontext: "+context+" - md5: "+md5);
        //Remove chunks
        for (ChunkMetadata chunkMetadata : metadataStorage.loadChunks(context, md5))
            binaryStorage.removeChunk(chunkMetadata);
        //Remove file
        if (fileMetadata.getStatus().getComplete())
            binaryStorage.removeFile(fileMetadata);
        metadataStorage.remove(context, md5);

        return Response.ok().build();
    }



    @POST
    @Path("process/{context}/{md5}")
    public Response postProcessFile(@PathParam("context") String context, @PathParam("md5") String md5) throws Exception {
        MetadataStorage metadataStorage = metadataFactory.getInstance();
        BinaryStorage binaryStorage = binaryFactory.getInstance();
        //Load file metadata
        FileMetadata fileMetadata = metadataStorage.load(context, md5);
        if (fileMetadata==null)
            throw new NoContentException("File not found\ncontext: "+context+" - md5: "+md5);
        FileStatus status = fileMetadata.getStatus();
        if (!status.getComplete())
            throw new NotAllowedException("File isn't complete");

        //Post process uploaded file
        for (PostUpload postUpload : processorsFactory.getPostUploadInstances(context))
            postUpload.fileUploaded(fileMetadata, binaryStorage);

        return Response.ok().build();
    }



    //Utils
/*
    InputStream getInputStream(HttpServletRequest request) throws Exception {

    }
*/
}
