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

@ProcessInfo(context = "adam.bulk", name = "AdamBulk", priority = 1)
public class AdamBulk implements PostUpload {

    private static final String PRIORITIES = "priorities";
    private static final String EXTENSION = "zip";
    private static final String UID = "adam_combined_priorities_table";
    @Inject private DataManager dataManager;
    @Inject private FileManager fileManager;
    @Inject private MetadataManager metadataManager;


    @Override
    public void chunkUploaded(ChunkMetadata metadata, MetadataStorage metadataStorage, BinaryStorage storage) throws Exception {
        //nothing to do
    }

    @Override
    public void fileUploaded(FileMetadata metadata, MetadataStorage metadataStorage, BinaryStorage storage, Map<String, Object> processingParams) throws Exception {

        InputStream zipFileInput = storage.readFile(metadata, null);
        //Start post processing
        mainLogic(zipFileInput);
    }

    public void mainLogic(InputStream zipFileInput) throws Exception {
        //Retrieve database connection
        Connection connection = dataManager.getConnection();
        //Create temporary folder with zip file content
        File tmpFolder = fileManager.createTmpFolder();
        try {
            File file = fileManager.saveFile(tmpFolder, zipFileInput, PRIORITIES,EXTENSION );
            //Unzip file into newly created folder
            Map<Files, File> recognizedFilesMap = fileManager.unzip(tmpFolder, new FileInputStream(file));
            //Check all needed files are present
            if (recognizedFilesMap.size()!=2)
                throw new NotAcceptableException("Some CSV file is missing");
            //Create tmp tables
            dataManager.createTmpTables(connection);
            // Upload data into database stage area
            dataManager.uploadCSV(recognizedFilesMap.get(Files.cpf), "fao_cpf_priorities", connection);
            dataManager.uploadCSV(recognizedFilesMap.get(Files.undaf), "recipient_undaf_priorities", connection);

            // Creation fo the final table to visualize
            dataManager.createFinalTable(connection);

            //Update metadata and cache
            metadataManager.updateMetadata(UID);
            metadataManager.updateCache(UID);
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
