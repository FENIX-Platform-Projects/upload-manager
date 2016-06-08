package org.fao.ess.uploader.oecd.policy.bulk.data.impl;

import org.fao.ess.uploader.oecd.policy.bulk.utils.DataSource;
import org.fao.fenix.commons.utils.CSVWriter;
import org.fao.fenix.commons.utils.FileUtils;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;

import javax.inject.Inject;
import java.io.File;
import java.io.StringWriter;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DataManager {
    private static String BACKUP_SCHEMA_NAME = "BACKUP";
    private static char CSV_SEPARATOR = ';';

    @Inject private DataSource dataSource;
    @Inject private FileUtils fileUtils;

/*
    public void uploadData(String source, File sourceData) throws Exception {
        Connection connection = dataSource.getConnection();
        try {
            String backupTableName = createBackupTable(source, connection);
            uploadCSV(sourceData, backupTableName, connection);
            publishData(backupTableName, connection);
            connection.commit();
        } catch (Exception ex) {
            connection.rollback();
        } finally {
            connection.close();
        }
    }
*/
    public String createBackupTable(String source, Connection connection) throws Exception {
        String tableName = BACKUP_SCHEMA_NAME+'.'+source+'_'+getTimeSuffix();

        //TODO

        return tableName;
    }

    public void uploadCSV(File sourceData, String backupTableName, Connection connection) throws Exception {
        StringBuilder query = new StringBuilder("COPY ").append(backupTableName).append(" FROM STDIN WITH CSV DELIMITER '").append(CSV_SEPARATOR).append("' QUOTE '\"' ENCODING 'UTF8'");
        CopyIn copier = ((PGConnection)connection).getCopyAPI().copyIn(query.toString());
        byte[] data = fileUtils.readTextFile(sourceData).getBytes();
        copier.writeToCopy(data,0,data.length);
        copier.flushCopy();
        copier.endCopy();
    }

    public void publishData(String backupTableName, Connection connection) throws Exception {

    }


    //Utils
    SimpleDateFormat timeSuffixFormat = new SimpleDateFormat("yyyyMMddhhmmss");
    private String getTimeSuffix(Date... date) {
        return timeSuffixFormat.format(date!=null && date.length>0 ? date[0] : new Date());
    }

}
