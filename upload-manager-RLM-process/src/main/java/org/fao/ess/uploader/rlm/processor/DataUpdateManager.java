package org.fao.ess.uploader.rlm.processor;

import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.process.PostUpload;
import org.fao.ess.uploader.core.process.ProcessInfo;
import org.fao.ess.uploader.core.storage.BinaryStorage;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;

@ApplicationScoped
@ProcessInfo(context = "c", name = "RLMDataUpdate", priority = 1)
public class DataUpdateManager implements PostUpload {
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
        RLMCSVParser parser = new RLMCSVParser(dataStream);
        //Get data database connection
        Connection connection = dataConnector.getConnection();
        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        //begin data transfer transaction
        try {
            connection.createStatement().executeUpdate("TRUNCATE TABLE master");
            PreparedStatement statement = connection.prepareStatement("INSERT INTO master (country,year,year_label,indicator,indicator_label,qualifier,value,um,source,topic,flag) VALUES (?,?,?,?,?,?,?,?,?,?,?)");
            int c=0;
            for (String[] record = parser.nextRow(); record!=null; record = parser.nextRow()) {
                statement.setString(1, record[0]);
                statement.setInt(2, record[1] != null ? new Integer(record[1]) : null);
                statement.setString(3, record[2]);
                statement.setString(4, record[3]);
                statement.setString(5, record[4]);
                statement.setString(6, record[5]);
                statement.setString(7, record[6]);
                statement.setString(8, record[7]);
                statement.setString(9, record[8]);
                statement.setString(10, record[9]);
                statement.setString(11, record[10]);
                statement.addBatch();
                if (c++==1000) { //Flush each 1000 rows
                    statement.executeBatch();
                    statement.clearBatch();
                    c=0;
                }
            }
            if (c>0)
                statement.executeBatch();
            //Update indicators
            connection.createStatement().executeUpdate("TRUNCATE TABLE codes_indicators");
            connection.createStatement().executeUpdate("INSERT INTO codes_indicators (indicator_parent_code, indicator_code, indicator_label, indicator_source) SELECT DISTINCT topic, indicator, indicator_label, source FROM master ORDER BY topic, indicator");
            connection.createStatement().executeUpdate("INSERT INTO codes_indicators (indicator_code, indicator_label) SELECT topic_code, topic_label FROM codes_topics");
            //Commit changes
            connection.commit();
        } finally {
            connection.setAutoCommit(autoCommit);
            connection.close();
        }
    }
    
}
