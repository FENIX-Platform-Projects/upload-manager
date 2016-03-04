package sftp;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PolicyDataManager {
    POLICYDataConnector policyDataConnector = new POLICYDataConnector();

    public Connection dbConnect(){

        //Get data database connection
        Connection connection = null;
        try {
            connection = policyDataConnector.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return connection;
    }

    public List<String> select(Connection connection, String query) throws Exception {
        boolean autoCommit = connection.getAutoCommit();
        ResultSet rs = null;
        Statement statement = null;
        List<String> selectValues = new ArrayList<String>();
        //begin data transfer transaction
        try {
            connection.setAutoCommit(false);
            System.out.println("query = "+query);
            statement = connection.createStatement();
            statement.executeQuery(query);
            rs = statement.getResultSet();
            while (rs.next()) {
                for (int i = 1 ; i < Integer.MAX_VALUE ; i++) {
                    try {
                        selectValues.add(rs.getString(i).trim());
                    } catch (SQLException e) {
                        break;
                    }
                }
            }
//            System.out.println(statement.getResultSet());
//            System.out.println(statement.getResultSet().getMetaData().getColumnCount());
        } finally {
            if(rs!=null){
                rs.close();
            }
            if(statement!=null){
                statement.close();
            }
            connection.setAutoCommit(autoCommit);
            connection.close();
        }

        return selectValues;
    }

    public List<String> update(Connection connection, String query) throws Exception {
        boolean autoCommit = connection.getAutoCommit();
        ResultSet rs = null;
        Statement statement = null;
        List<String> selectValues = new ArrayList<String>();
        //begin data transfer transaction
        try {
            connection.setAutoCommit(false);
            System.out.println("query = "+query);
            statement = connection.createStatement();
            statement.executeQuery(query);
            rs = statement.getResultSet();
            while (rs.next()) {
                for (int i = 1 ; i < Integer.MAX_VALUE ; i++) {
                    try {
                        selectValues.add(rs.getString(i).trim());
                    } catch (SQLException e) {
                        break;
                    }
                }
            }
//            System.out.println(statement.getResultSet());
//            System.out.println(statement.getResultSet().getMetaData().getColumnCount());
        } finally {
            if(rs!=null){
                rs.close();
            }
            if(statement!=null){
                statement.close();
            }
            connection.setAutoCommit(autoCommit);
            connection.close();
        }

        return selectValues;
    }

    public void updateLinkPdf(Connection connection, String fileName, String policyId) throws Exception {

        PreparedStatement preparedStatementSelect = null;
        PreparedStatement preparedStatementUpdate = null;
        boolean duplicate = false;

        String selectQuery = "SELECT link_pdf FROM policytable where policy_id = "+ Integer.parseInt(policyId);

        try {
            String linkPdfToAdd = "";
            connection.setAutoCommit(false);

            preparedStatementSelect = connection.prepareStatement(selectQuery);
            preparedStatementSelect.executeQuery();

            ResultSet rs = preparedStatementSelect.getResultSet();
            String selectValue = "";
            String selectValueArray[] = new String[10];

            while (rs.next()) {
                selectValue= rs.getString(0).trim();
                selectValueArray = selectValue.split(";");
                break;
            }

            int i=0;
            boolean toAdd = false;
            if((selectValue!=null)&&(selectValue.length()>1)) {
                for (i = 0; i < selectValueArray.length; i++) {
                    if (selectValueArray[i].equals(fileName)) {
                        break;
                    }
                }

                System.out.println("selectValue= " + selectValue);
                //Add another link to the list of link
                if(i<selectValueArray.length){
                    //This file name is already in the database
                    duplicate = true;
                }
                else{
                    linkPdfToAdd = selectValue + ";";
                }
            }

            if(!duplicate){
                linkPdfToAdd += fileName;

                String updateTableSQL = "UPDATE policytable SET link_pdf= '" + linkPdfToAdd + "' where policy_id = " + Integer.parseInt(policyId);

                System.out.println();

                preparedStatementUpdate = connection.prepareStatement(updateTableSQL);
                preparedStatementUpdate.executeUpdate();

                connection.commit();

                System.out.println("Done!");
            }

        } catch (SQLException e) {

            System.out.println(e.getMessage());
            connection.rollback();

        } finally {

            if (preparedStatementSelect != null) {
                preparedStatementSelect.close();
            }

            if (preparedStatementUpdate != null) {
                preparedStatementUpdate.close();
            }

            if (connection != null) {
                connection.close();
            }
        }

    }

//    public void fileUploaded(FileMetadata metadata, BinaryStorage storage) throws Exception {
//        //Load data stream
//        InputStream dataStream = storage.readFile(metadata, null);
//        //Prepare CSV parser
//        RLMCSVParser parser = new RLMCSVParser(dataStream);
//        //Get data database connection
//        Connection connection = dataConnector.getConnection();
//        boolean autoCommit = connection.getAutoCommit();
//        connection.setAutoCommit(false);
//        //begin data transfer transaction
//        try {
//            connection.createStatement().executeUpdate("TRUNCATE TABLE master");
//            PreparedStatement statement = connection.prepareStatement("INSERT INTO master (country,year,year_label,indicator,indicator_label,qualifier,value,um,source,topic,flag) VALUES (?,?,?,?,?,?,?,?,?,?,?)");
//            int c=0;
//            for (String[] record = parser.nextRow(); record!=null; record = parser.nextRow()) {
//                statement.setString(1, record[0]);
//                statement.setInt(2, record[1] != null ? new Integer(record[1]) : null);
//                statement.setString(3, record[2]);
//                statement.setString(4, record[3]);
//                statement.setString(5, record[4]);
//                statement.setString(6, record[5]);
//                statement.setString(7, record[6]);
//                statement.setString(8, record[7]);
//                statement.setString(9, record[8]);
//                statement.setString(10, record[9]);
//                statement.setString(11, record[10]);
//                statement.addBatch();
//                if (c++==1000) { //Flush each 1000 rows
//                    statement.executeBatch();
//                    statement.clearBatch();
//                    c=0;
//                }
//            }
//            if (c>0)
//                statement.executeBatch();
//            //Update indicators
//            connection.createStatement().executeUpdate("TRUNCATE TABLE codes_indicators");
//            connection.createStatement().executeUpdate("INSERT INTO codes_indicators (indicator_parent_code, indicator_code, indicator_label, indicator_source) SELECT DISTINCT topic, indicator, indicator_label, source FROM master ORDER BY topic, indicator");
//            connection.createStatement().executeUpdate("INSERT INTO codes_indicators (indicator_code, indicator_label) SELECT topic_code, topic_label FROM codes_topics");
//            //Commit changes
//            connection.commit();
//        } finally {
//            connection.setAutoCommit(autoCommit);
//            connection.close();
//        }
//    }
}
