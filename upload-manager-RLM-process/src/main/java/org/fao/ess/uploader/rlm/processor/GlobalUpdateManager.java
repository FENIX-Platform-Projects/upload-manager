package org.fao.ess.uploader.rlm.processor;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.init.UploaderConfig;
import org.fao.ess.uploader.core.storage.BinaryStorage;
import org.fao.ess.uploader.core.upload.PostUpload;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.InputStream;
import java.io.InputStreamReader;

@ApplicationScoped
public class GlobalUpdateManager implements PostUpload {
    @Inject RLMDataConnector dataConnector;
    @Inject D3SClient d3sClient;

    @Override
    public void chunkUploaded(ChunkMetadata metadata, BinaryStorage storage) throws Exception {
        //Nothing to do here
    }

    @Override
    public void fileUploaded(FileMetadata metadata, BinaryStorage storage) throws Exception {
        //Get data connection
        dataConnector.getConnection();


        //Load csv and write data
        InputStream dataStream = storage.readFile(metadata, null);
        CSVParser parser = new CSVParser(new InputStreamReader(dataStream), CSVFormat.DEFAULT.withDelimiter(';').withIgnoreEmptyLines().withQuote('"').withHeader());
        for (CSVRecord record : parser) {
            String val1 = record.get(1);
            record.iterator();
            record.size();

        }

        //Update D3S resources
        d3sClient.sendDataUpdatedSignal();
    }
}
