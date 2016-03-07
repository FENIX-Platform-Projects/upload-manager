package sftp;

import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.metadata.MetadataStorage;
import org.fao.ess.uploader.core.metadata.MetadataStorageFactory;
import org.fao.ess.uploader.core.process.PostUpload;
import org.fao.ess.uploader.core.process.ProcessInfo;
import org.fao.ess.uploader.core.storage.BinaryStorage;
import org.fao.ess.uploader.core.storage.BinaryStorageFactory;
import org.fao.ess.uploader.core.upload.FileService;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

@ProcessInfo(context = "policy", name = "PolicyData", priority = 1)
public class DataManager implements PostUpload {

    @Override
    public void chunkUploaded(ChunkMetadata metadata, MetadataStorage metadataStorage, BinaryStorage storage) throws Exception {
        //Nothing to do here
    }

    @Override
    public void fileUploaded(FileMetadata metadata, MetadataStorage metadataStorage, BinaryStorage storage, Map<String, Object> processingParams) throws Exception {
        transferFile(
                processingParams!=null ? (String)processingParams.get("policy") : null,
                metadata.getName()!=null ? metadata.getName() : (String)processingParams.get("name"),
                storage.readFile(metadata, null)
        );
    }


    private void transferFile(String policyId, String fileName, InputStream dataStream) throws Exception {
        //Main Properties file
        SftpPropertiesValues properties = new SftpPropertiesValues();
        Properties prop = properties.getPropValues('m');
        //Transfer file
        new SftpUpload().connect(prop, null, null, null, dataStream, fileName, policyId);
    }
    
}
