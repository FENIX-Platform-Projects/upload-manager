package org.fao.ess.uploader.core.process;

import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.dto.FileStatus;
import org.fao.ess.uploader.core.dto.ProcessMetadata;
import org.fao.ess.uploader.core.metadata.MetadataStorage;
import org.fao.ess.uploader.core.metadata.MetadataStorageFactory;
import org.fao.ess.uploader.core.storage.BinaryStorage;
import org.fao.ess.uploader.core.storage.BinaryStorageFactory;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.NoContentException;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Path("process")
public class ProcessService {
    @Inject MetadataStorageFactory metadataFactory;
    @Inject BinaryStorageFactory binaryFactory;
    @Inject ProcessFactory processorsFactory;


    @POST
    @Path("{context}/{md5}")
    public Response createProcess(@PathParam("context") String context, @PathParam("md5") String md5, Map<String, Object> processingParams) throws Exception {
        MetadataStorage metadataStorage = metadataFactory.getInstance();
        BinaryStorage binaryStorage = binaryFactory.getInstance();
        //Load file metadata
        FileMetadata fileMetadata = metadataStorage.load(context, md5);
        if (fileMetadata==null)
            throw new NoContentException("File not found\ncontext: "+context+" - md5: "+md5);
        FileStatus status = fileMetadata.getStatus();
        if (!status.getComplete())
            throw new NotAllowedException("File isn't complete");

        //Manage flow
        Collection<ProcessMetadata> flow = processorsFactory.getPostUploadInstances(fileMetadata);
        if (flow.size()>0) {
            //Remove existing flow metadata
            metadataStorage.removeProcess(context, md5);
            //Store flow metadata
            for (ProcessMetadata metadata : flow)
                metadataStorage.save(metadata);
            //Start flow execution
            new ProcessFlow(metadataStorage, binaryStorage, fileMetadata, flow, processingParams).start(500);
        }

        return Response.ok().build();
    }

    @GET
    @Path("{context}/{md5}")
    @Produces(MediaType.APPLICATION_JSON)
    public Collection<ProcessMetadata> processStatus (@PathParam("context") String context, @PathParam("md5") String md5) throws Exception {
        MetadataStorage metadataStorage = metadataFactory.getInstance();
        //Load file metadata
        FileMetadata fileMetadata = metadataStorage.load(context, md5);
        if (fileMetadata==null)
            throw new NoContentException("File not found\ncontext: "+context+" - md5: "+md5);
        FileStatus status = fileMetadata.getStatus();
        if (!status.getComplete())
            throw new NotAllowedException("File isn't complete");
        //Load process status
        return metadataStorage.loadProcesses(context,md5);
    }

}
