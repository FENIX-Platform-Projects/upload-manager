package org.fao.ess.uploader.adam.bulk;

import org.fao.ess.uploader.adam.dto.Files;
import org.fao.ess.uploader.adam.impl.DataManager;
import org.fao.ess.uploader.adam.impl.FileManager;
import org.fao.ess.uploader.adam.impl.MetadataManager;
import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.metadata.MetadataStorage;
import org.fao.ess.uploader.core.process.PostUpload;
import org.fao.ess.uploader.core.process.ProcessInfo;
import org.fao.ess.uploader.core.storage.BinaryStorage;

import javax.inject.Inject;
import javax.ws.rs.NotAcceptableException;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.util.Map;

@ProcessInfo(context = "adam.dataset", name = "AdamDS", priority = 1)
public class AdamDS implements PostUpload {
    private static final String EXTENSION = "csv";

    @Inject private DataManager dataManager;
    @Inject private FileManager fileManager;
    @Inject private MetadataManager metadataManager;




    @Override
    public void chunkUploaded(ChunkMetadata metadata, MetadataStorage metadataStorage, BinaryStorage storage) throws Exception {
        //Nothing to do
    }

    @Override
    public void fileUploaded(FileMetadata metadata, MetadataStorage metadataStorage, BinaryStorage storage, Map<String, Object> processingParams) throws Exception {
        InputStream csvFileInput = storage.readFile(metadata, null);

        if(processingParams == null || processingParams.keySet().size() == 0 || processingParams.containsKey("uid") || processingParams.get("uid") == null || processingParams.get("uid").equals(""))
            throw new Exception("Wrong configuration: uid parameter misses or it is wrong");
        mainLogic(csvFileInput, processingParams.get("uid").toString());

    }

    public void mainLogic(InputStream csvFile, String uid) throws Exception {
        //Retrieve database connection
        Connection connection = dataManager.getConnection();
        //Create temporary folder with zip file content
        File tmpFolder = fileManager.createTmpFolder();
        try {
            File file = fileManager.saveFile(tmpFolder, csvFile, uid,EXTENSION );
            //Map the files to check if the name si right
            Map<Files, File> recognizedFilesMap = fileManager.mapFiles(file);
            //Check the file is present
            if (recognizedFilesMap.size()==0)
                throw new NotAcceptableException("CSV file is missing");
            //Create tmp tables
            dataManager.cleanTmpData(connection, uid);
            // Upload data into
            dataManager.uploadCSV(file, uid, connection);
            //Update metadata and cache
            metadataManager.updateMetadata("adam_"+uid);
            metadataManager.updateCache("adam_"+uid);
            //Commit database changes
            connection.commit();
        } catch (Exception ex) {
            connection.rollback();
            throw ex;
        } finally {
            fileManager.removeTmpFolder(tmpFolder);
            connection.close();
        }
    }
}
