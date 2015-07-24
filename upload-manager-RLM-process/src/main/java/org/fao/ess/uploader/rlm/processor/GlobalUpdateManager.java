package org.fao.ess.uploader.rlm.processor;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.storage.BinaryStorage;
import org.fao.ess.uploader.core.upload.PostUpload;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;

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
        //Load data stream
        InputStream dataStream = storage.readFile(metadata, null);
        //Prepare CSV parser
        CSVParser parser = new CSVParser(
                new InputStreamReader(dataStream),
                CSVFormat.DEFAULT.withDelimiter(';')
                        .withIgnoreEmptyLines()
                        .withQuote('"')
                        .withHeader()
        );
        //Get data database connection
        Connection connection = dataConnector.getConnection();
        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        //begin data transfer transaction
        try {
            connection.createStatement().executeUpdate("TRUNCATE TABLE master");
            PreparedStatement statement = connection.prepareStatement("INSERT INTO master (country,year,year_label,indicator,indicator_label,qualifier,value,um,source,topic) VALUES (?,?,?,?,?,?,?,?,?,?)");
            int c=0;
            for (CSVRecord record : parser) {
                statement.setString(1, record.get(0));
                statement.setInt(2, record.get(1) != null ? new Integer(record.get(1)) : null);
                statement.setString(3, record.get(2));
                statement.setString(4, record.get(3));
                statement.setString(5, record.get(4));
                statement.setString(6, record.get(5));
                statement.setString(7, record.get(6));
                statement.setString(8, record.get(7));
                statement.setString(9, record.get(8));
                statement.setString(10, record.get(9));
                statement.addBatch();
                if (c++==1000) { //Flush each 1000 rows
                    statement.executeBatch();
                    statement.clearBatch();
                    c=0;
                }
            }
            if (c>0)
                statement.executeBatch();
            connection.commit();
        } finally {
            connection.setAutoCommit(autoCommit);
            connection.close();
        }
        //Update D3S resources
        d3sClient.sendDataUpdatedSignal();
    }
    
}
