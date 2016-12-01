package org.fao.ess.uploader.adam.utils.data;

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

   /* public void validateSurveyData(Connection connection) throws Exception {
        //Verify data food codes are assigned to a group
        StringBuilder error = new StringBuilder();
        for (ResultSet resultSet = connection.createStatement().executeQuery(Queries.getUnexistingFoodGroup.getQuery()); resultSet.next(); error.append('\n').append(resultSet.getString(1)));
        if (error.length()>0)
            throw new NotAcceptableException("Data have food codes with no group assigned: "+error.toString());
    }*/

  /*  public void cleanTmpData(Connection connection) throws Exception {
        connection.createStatement().executeUpdate(Queries.cleanCPFPriorities.getQuery());
        connection.createStatement().executeUpdate(Queries.cleanRecipientUndafPriorities.getQuery());
    }*/

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

