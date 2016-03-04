package sftp;

import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.process.PostUpload;
import org.fao.ess.uploader.core.process.ProcessInfo;
import org.fao.ess.uploader.core.storage.BinaryStorage;

import javax.enterprise.context.ApplicationScoped;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

@ApplicationScoped
@ProcessInfo(context = "policy", name = "PolicyData", priority = 1)
public class DataManager implements PostUpload {

    @Override
    public void chunkUploaded(ChunkMetadata metadata, BinaryStorage storage) throws Exception {
        //Nothing to do here
    }

    @Override
    public void fileUploaded(FileMetadata metadata, BinaryStorage storage, Map<String, Object> processingParams) throws Exception {
        //Load data stream
        InputStream dataStream = storage.readFile(metadata, null);
        //Load policy ID
        String policyId = processingParams!=null ? (String)processingParams.get("policy") : null;
        String fileName = metadata.getName()!=null ? metadata.getName() : (String)processingParams.get("name");
        //Main Properties file
        SftpPropertiesValues properties = new SftpPropertiesValues();
        Properties prop = properties.getPropValues('m');
        //Transfer file
        new SftpUpload().connect(prop, null, null, null, dataStream, fileName, policyId);
    }
    
}
