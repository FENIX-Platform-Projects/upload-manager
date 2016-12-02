package org.fao.ess.uploader.adam.impl;

import org.fao.ess.uploader.adam.dto.Queries;
import org.fao.ess.uploader.adam.utils.connection.DataSource;
import org.fao.fenix.commons.utils.FileUtils;
import org.fao.fenix.commons.utils.UIDUtils;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;

import javax.inject.Inject;
import javax.ws.rs.NotAcceptableException;
import java.io.File;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;

public class DataManager {
    private static char CSV_SEPARATOR = ',';

    @Inject private DataSource dataSource;
    @Inject private DatabaseUtils databaseUtils;
    @Inject private FileUtils fileUtils;
    @Inject private UIDUtils uidUtils;


    public Connection getConnection() throws Exception {
        return dataSource.getConnection();
    }

    public void uploadCSV(File csvData, String tmpTableName, Connection connection) throws Exception {
        StringBuilder query = new StringBuilder("COPY ").append(tmpTableName).append(" FROM STDIN WITH CSV HEADER DELIMITER '").append(CSV_SEPARATOR).append("' QUOTE '\"' ENCODING 'UTF8'");
        CopyIn copier = ((PGConnection)connection).getCopyAPI().copyIn(query.toString());
        byte[] data = fileUtils.readTextFile(csvData).getBytes();
        copier.writeToCopy(data,0,data.length);
        copier.flushCopy();
        copier.endCopy();
    }



    public void cleanTmpData(Connection connection, String uid) throws Exception {
        switch (uid){
            case "donors_gni":
                connection.createStatement().executeUpdate(Queries.cleanDonorsGNI.getQuery());
                break;

            case "country_indicators":
                connection.createStatement().executeUpdate(Queries.cleanAdamCountryIndicator.getQuery());
                break;

            default:
                break;
        }
    }

    public void createTmpTables (Connection connection) throws Exception{
        connection.createStatement().executeUpdate(Queries.createCPFPriorites.getQuery());
        connection.createStatement().executeUpdate(Queries.createUndafPriorites.getQuery());
    }

    public void createFinalTable(Connection connection) throws Exception {
        //Update survey data with raw tables content
        CallableStatement callStatement = connection.prepareCall(Queries.callFunctionCPFUndaf.getQuery());
        callStatement.execute();
        callStatement = connection.prepareCall(Queries.callFunctionPrioritiesTable.getQuery());
        callStatement.execute();
    }

}

