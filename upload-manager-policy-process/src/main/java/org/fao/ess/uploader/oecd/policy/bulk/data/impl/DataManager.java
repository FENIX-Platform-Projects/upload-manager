package org.fao.ess.uploader.oecd.policy.bulk.data.impl;

import org.fao.ess.uploader.oecd.policy.bulk.attachments.dto.AttachmentProperties;
import org.fao.ess.uploader.oecd.policy.bulk.utils.DataSource;
import org.fao.fenix.commons.utils.FileUtils;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;

import javax.inject.Inject;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.*;

public class DataManager {
    private static String BACKUP_SCHEMA_NAME = "BACKUP";
    private static char CSV_SEPARATOR = ',';

    @Inject private DataSource dataSource;
    @Inject private FileUtils fileUtils;

    public Connection getConnection() throws Exception {
        return dataSource.getConnection();
    }

    public String createBackupTable(String source, Connection connection) throws Exception {
        String tableName = BACKUP_SCHEMA_NAME+'.'+source+'_'+getTimeSuffix();
        connection.createStatement().executeUpdate(
                "create table " + tableName + " (\n" +
                "    Commodity_ID integer not null references commlistwithid (commodity_id),\n" +
                "    Country_Code integer not null check (Country_Code >= 0),\n" +
                "    Country_Name varchar not null,\n" +
                "    Subnational_Code integer not null,\n" +
                "    Subnational_Name varchar not null,\n" +
                "    CommodityDomain_Code integer not null check (Country_Code >= 0),\n" +
                "    CommodityDomain_Name varchar not null,\n" +
                "    PolicyDomain_Code integer not null check (Country_Code >= 0),\n" +
                "    PolicyDomain_Name varchar not null,\n" +
                "    PolicyType_Code integer not null check (Country_Code >= 0),\n" +
                "    PolicyType_Name varchar not null,\n" +
                "    PolicyMeasure_Code integer not null check (Country_Code >= 0),\n" +
                "    PolicyMeasure_Name varchar not null,\n" +
                "    CommodityClass_Code integer not null check (Country_Code >= 0),\n" +
                "    CommodityClass_Name varchar not null,\n" +
                "    Condition_Code integer,\n" +
                "    Condition varchar,\n" +
                "    IndividualPolicy_Code integer,\n" +
                "    IndividualPolicy_Name varchar,\n" +
                "    Start_Date date,\n" +
                "    End_Date date,\n" +
                "    Element_Code integer,\n" +
                "    Policy_Element varchar,\n" +
                "    HS_Code varchar,\n" +
                "    HS_Version varchar,\n" +
                "    HS_Suffix varchar,\n" +
                "    Description varchar,\n" +
                "    Short_Description varchar,\n" +
                "    Shared_Group_Code varchar,\n" +
                "    Second_Generation_Specific varchar,\n" +
                "    Imposed_End_Date varchar,\n" +
                "    Units varchar,\n" +
                "    Value varchar,\n" +
                "    Value_Text varchar,\n" +
                "    Value_Type varchar,\n" +
                "    Exemptions varchar,\n" +
                "    Notes varchar,\n" +
                "    Measure_Description varchar,\n" +
                "    Link varchar,\n" +
                "    Source varchar,\n" +
                "    Date_Of_Publication date,\n" +
                "    Title_Of_Notice varchar,\n" +
                "    Legal_Basis_Name varchar,\n" +
                "    Benchmark_Tax varchar,\n" +
                "    Benchmark_Product varchar,\n" +
                "    Tax_Rate_Biofuel varchar,\n" +
                "    Tax_Rate_Benchmark varchar,\n" +
                "    Start_Date_Tax varchar,\n" +
                "    Benchmark_Link varchar,\n" +
                "    Benchmark_Link_pdf varchar,\n" +
                "    Product_Original_HS varchar,\n" +
                "    Product_Original_Name varchar,\n" +
                "    Type_Of_Change_Code double precision,\n" +
                "    Type_Of_Change_Name varchar,\n" +
                "    Original_Dataset varchar,\n" +
                "    Condition_Exists varchar not null,\n" +
                "    MinAVTariffValue varchar,\n" +
                "    MaxAVTariffValue varchar,\n" +
                "    CountAVTariff varchar,\n" +
                "    CountNAVTariff varchar\n" +
                ")"
        );

        return tableName;
    }

    public void uploadCSV(InputStream sourceData, String backupTableName, Connection connection) throws Exception {
        StringBuilder query = new StringBuilder("COPY ").append(backupTableName).append(" FROM STDIN WITH CSV DELIMITER '").append(CSV_SEPARATOR).append("' QUOTE '\"' ENCODING 'UTF8'");
        CopyIn copier = ((PGConnection)connection).getCopyAPI().copyIn(query.toString());
        byte[] data = fileUtils.readTextFile(sourceData).getBytes();
        copier.writeToCopy(data,0,data.length);
        copier.flushCopy();
        copier.endCopy();
    }

    public Map<Integer, Integer> getSubnationalCodesTranscodeMap(String backupTableName, Connection connection) throws Exception {
        Map<Integer, Integer> transcodeMap = new HashMap<>();
        //Get next new negative code
        ResultSet data = connection.createStatement().executeQuery("select min(subnational_code)-1 as code from customsubnationaltable");
        Integer nextNegativeCode = data.next() ? data.getInt(1) : -1;
        //Get and work on csv negative subnational codes
        data = connection.createStatement().executeQuery("select subnational_code, subnational_name, country_code from "+backupTableName+" where subnational_code < 0");
        while (data.next()) {
            PreparedStatement statement = connection.prepareStatement("select subnational_code from customsubnationaltable where btrim(lower(subnational_name)) = ?");
            //Set an existing subnational code (by label)
            statement.setString(1,data.getString(2).trim().toLowerCase());
            ResultSet existingSubnationalCode = statement.executeQuery();
            if (existingSubnationalCode.next())
                transcodeMap.put(data.getInt(1), existingSubnationalCode.getInt(1));
            else { //Or insert and set a new one
                statement = connection.prepareStatement("INSERT INTO customsubnationaltable (subnational_code, subnational_name, country_code) values (?,?,?)");
                statement.setInt(1, nextNegativeCode);
                statement.setString(2, data.getString(2).trim());
                statement.setInt(3, data.getInt(3));
                statement.executeUpdate();
                transcodeMap.put(data.getInt(1), nextNegativeCode--);
            }
        }
        //Update negative codes into csv table
        for (Map.Entry<Integer,Integer> transcodeMapEntry : transcodeMap.entrySet()) {
            PreparedStatement statement = connection.prepareStatement("UPDATE "+backupTableName+" SET subnational_code = ? WHERE subnational_code = ?");
            statement.setInt(1, transcodeMapEntry.getValue());
            statement.setInt(2, transcodeMapEntry.getKey());
        }
        //Return transcode map
        return transcodeMap;
    }

    public Collection<String> createMetadataId (String backupTableName, Connection connection) throws Exception {
        Collection<String> metadataIdList = new TreeSet<>();
        String indexSequenceName = backupTableName+"_seq";
        String step1TableName = backupTableName+"_step_1";
        //Create sequence for the row index column
        connection.createStatement().executeUpdate("create sequence "+indexSequenceName);
        try {
            //create step 1 table with metadata id and row index
            connection.createStatement().executeUpdate(
                    "create table "+step1TableName+" as\n" +
                    "    select\n" +
                    "        nextval ('"+indexSequenceName+"') as index,\n" +
                    "        'POLICY_'||Country_Code||'_'||Subnational_Code||'_'||CommodityDomain_Code||'_'||CommodityClass_Code||'_'||PolicyDomain_Code||'_'||PolicyType_Code||'_'||PolicyMeasure_Code||'_'||Condition_Exists as metadata_id,\n" +
                    "        Commodity_ID,Country_Code,Country_Name,Subnational_Code,Subnational_Name,CommodityDomain_Code,CommodityDomain_Name,PolicyDomain_Code,PolicyDomain_Name,PolicyType_Code,PolicyType_Name,PolicyMeasure_Code,PolicyMeasure_Name,CommodityClass_Code,CommodityClass_Name,Condition_Code,Condition,IndividualPolicy_Code,IndividualPolicy_Name,Start_Date,End_Date,Element_Code,Policy_Element,HS_Code,HS_Version,HS_Suffix,Description,Short_Description,Shared_Group_Code,Second_Generation_Specific,Imposed_End_Date,Units,Value,Value_Text,Value_Type,Exemptions,Notes,Measure_Description,Link,Source,Date_Of_Publication,Title_Of_Notice,Legal_Basis_Name,Benchmark_Tax,Benchmark_Product,Tax_Rate_Biofuel,Tax_Rate_Benchmark,Start_Date_Tax,Benchmark_Link,Benchmark_Link_pdf,Product_Original_HS,Product_Original_Name,Type_Of_Change_Code,Type_Of_Change_Name,Original_Dataset,Condition_Exists,MinAVTariffValue,MaxAVTariffValue,CountAVTariff,CountNAVTariff\n" +
                    "    from "+backupTableName
            );
        } finally {
            //Remove sequence after table creation
            connection.createStatement().executeUpdate("drop sequence "+indexSequenceName);
        }
        //Return newly created metadataId
        ResultSet data = connection.createStatement().executeQuery("SELECT metadata_id from "+step1TableName+" order by index");
        while (data.next())
            metadataIdList.add(data.getString(1));
        return metadataIdList;
    }

    public void removeStrictlySourceRelatedData(String source, String backupTableName, Connection connection) throws Exception {
        //Remove cpl
        PreparedStatement statement = connection.prepareStatement("delete from mastertable where cpl_id in (select cpl_id from (select distinct cpl_id, source from policysource) ps group by cpl_id having count(*) = 1 and first (source) = ? )");
        statement.setString(1, source);
        statement.executeUpdate();
        statement = connection.prepareStatement("delete from mastertableb where cpl_id in (select cpl_id from (select distinct cpl_id, source from policysource) ps group by cpl_id having count(*) = 1 and first (source) = ? )");
        statement.setString(1, source);
        statement.executeUpdate();
        //Remove policy
        statement = connection.prepareStatement("delete from pdfuploadinfo where policy_id in (select policy_id from policysource where source = ?)");
        statement.setString(1, source);
        statement.executeUpdate();
        statement = connection.prepareStatement("delete from metadata_id_linked_to_cpl_id where policy_id in (select policy_id from policysource where source = ?)");
        statement.setString(1, source);
        statement.executeUpdate();
        statement = connection.prepareStatement("delete from policytable where policy_id in (select policy_id from policysource where source = ?)");
        statement.setString(1, source);
        statement.executeUpdate();
        //Remove source links
        statement = connection.prepareStatement( "delete from policysource where source = ?" );
        statement.setString(1, source);
        statement.executeUpdate();
    }

    public Integer[] createPolicyId(String backupTableName, Connection connection) throws Exception {
        String policySequenceName = "flow_policy_id";
        String step1TableName = backupTableName+"_step_1";
        String step2TableName = backupTableName+"_step_1";
        try {
            //init policy id sequence
            connection.createStatement().executeUpdate("select setval ('" + policySequenceName + "', max(policy_id)) from policytable");
            //create step 2 table with existing cpl id and null policy id
            connection.createStatement().executeUpdate(
                    "create table "+step2TableName+" as\n" +
                    "    select s.index, s.metadata_id, o.cpl_id, cast (null as integer) as policy_id, cast (null as varchar) as link_pdf, s.Commodity_ID, s.Country_Code, s.Country_Name, s.Subnational_Code, s.Subnational_Name, s.CommodityDomain_Code, s.CommodityDomain_Name, s.PolicyDomain_Code, s.PolicyDomain_Name, s.PolicyType_Code, s.PolicyType_Name, s.PolicyMeasure_Code, s.PolicyMeasure_Name, s.CommodityClass_Code, s.CommodityClass_Name, s.Condition_Code, s.Condition, s.IndividualPolicy_Code, s.IndividualPolicy_Name, s.Start_Date, s.End_Date, s.Element_Code, s.Policy_Element, s.HS_Code, s.HS_Version, s.HS_Suffix, s.Description, s.Short_Description, s.Shared_Group_Code, s.Second_Generation_Specific, s.Imposed_End_Date, s.Units, s.Value, s.Value_Text, s.Value_Type, s.Exemptions, s.Notes, s.Measure_Description, s.Link, s.Source, s.Date_Of_Publication, s.Title_Of_Notice, s.Legal_Basis_Name, s.Benchmark_Tax, s.Benchmark_Product, s.Tax_Rate_Biofuel, s.Tax_Rate_Benchmark, s.Start_Date_Tax, s.Benchmark_Link, s.Benchmark_Link_pdf, s.Product_Original_HS, s.Product_Original_Name, s.Type_Of_Change_Code, s.Type_Of_Change_Name, s.Original_Dataset, s.Condition_Exists, s.MinAVTariffValue, s.MaxAVTariffValue, s.CountAVTariff, s.CountNAVTariff\n" +
                    "    from   "+step1TableName+" s\n" +
                    "           left outer join\n" +
                    "           (select a.cpl_id, Commodity_ID, Country_Code, Country_Name, b.Subnational_Code, Subnational_Name, CommodityDomain_Code, CommodityDomain_Name, PolicyDomain_Code, PolicyDomain_Name, PolicyType_Code, PolicyType_Name, PolicyMeasure_Code, PolicyMeasure_Name, CommodityClass_Code, CommodityClass_Name, Condition_Code, Condition, IndividualPolicy_Code, IndividualPolicy_Name from mastertable a join mastertableb b on (a.cpl_id = b.cpl_id)) o\n" +
                    "           on (s.Subnational_Code=o.Subnational_Code AND s.Commodity_ID=o.Commodity_ID AND s.Country_Code=o.Country_Code AND s.Country_Name=o.Country_Name AND s.CommodityDomain_Code=o.CommodityDomain_Code AND s.CommodityDomain_Name=o.CommodityDomain_Name AND s.PolicyDomain_Code=o.PolicyDomain_Code AND s.PolicyDomain_Name=o.PolicyDomain_Name AND s.PolicyType_Code=o.PolicyType_Code AND s.PolicyType_Name=o.PolicyType_Name AND s.PolicyMeasure_Code=o.PolicyMeasure_Code AND s.PolicyMeasure_Name=o.PolicyMeasure_Name AND s.CommodityClass_Code=o.CommodityClass_Code AND s.CommodityClass_Name=o.CommodityClass_Name AND s.Condition_Code=o.Condition_Code AND s.Condition=o.Condition AND s.IndividualPolicy_Code=o.IndividualPolicy_Code AND s.IndividualPolicy_Name=o.IndividualPolicy_Name)"
            );
        } finally {
            //remove step 1 table
            connection.createStatement().executeUpdate("drop table " + step1TableName);
        }
        try {
            //update step 2 table with new policy id
            connection.createStatement().executeUpdate("update "+step2TableName+" set policy_id = nextval ('"+policySequenceName+"')");
            //Return policy id ordered by row index
            Collection<Integer> policyIdList = new LinkedList<>();
            ResultSet data = connection.createStatement().executeQuery("SELECT policy_id from "+step2TableName+" ORDER BY index");
            while (data.next())
                policyIdList.add(data.getInt(1));
            return policyIdList.toArray(new Integer[policyIdList.size()]);
        } catch (Exception ex) {
            //remove step 2 table in case error occur
            connection.createStatement().executeUpdate("drop table " + step2TableName);
            throw ex;
        }
    }


    public void updateAttachmentsData(Collection<AttachmentProperties> attachments, String backupTableName, Connection connection) throws Exception {
        if (attachments!=null && attachments.size()>0) {
            String step2TableName = backupTableName + "_step_1";
            try {
                Map<Integer, StringBuilder> attachmentsNameMap = new HashMap<>();
                PreparedStatement statement = connection.prepareStatement("INSERT INTO pdfuploadinfo (policy_id, file_name, file_md5) VALUES (?,?,?)");
                for (AttachmentProperties attachmentProperties : attachments) {
                    //update pdfuploadinfo table
                    statement.setInt(1, attachmentProperties.getPolicyId());
                    statement.setString(2, attachmentProperties.getFileName());
                    statement.setString(3, attachmentProperties.getMd5());
                    statement.addBatch();
                    //collect attachments name
                    StringBuilder attachmentsName = attachmentsNameMap.get(attachmentProperties.getPolicyId());
                    if (attachmentsName == null)
                        attachmentsNameMap.put(attachmentProperties.getPolicyId(), attachmentsName = new StringBuilder());
                    attachmentsName.append(attachmentProperties.getFileName()).append(';');
                }
                statement.executeBatch();
                //Update step 2 table link_pdf column
                for (Map.Entry<Integer, StringBuilder> attachmentsNameMapEntry : attachmentsNameMap.entrySet()) {
                    statement = connection.prepareStatement("UPDATE "+step2TableName+" SET link_pdf = ? WHERE policy_id = ?");
                    statement.setString(1, attachmentsNameMapEntry.getValue().substring(0, attachmentsNameMapEntry.getValue().length()-1));
                    statement.setInt(2, attachmentsNameMapEntry.getKey());
                    statement.executeUpdate();
                }
            } catch (Exception ex) {
                //remove step 2 table in case error occur
                connection.createStatement().executeUpdate("drop table " + step2TableName);
                throw ex;
            }
        }
    }

    public void finishDataPublication (String source, String backupTableName, Connection connection) throws Exception {
        String cplSequenceName = "flow_cpl_id";
        String step2TableName = backupTableName + "_step_1";
        try {
            //Update policysource table with already existing cpl
            PreparedStatement statement = connection.prepareStatement("insert into policysource select cpl_id, policy_id, ? from "+step2TableName+" where cpl_id is not null");
            statement.setString(1, source);
            statement.executeUpdate();
            //Update policytable table with policies assigned to already existing cpl
            connection.createStatement().executeUpdate("insert into policytable select metadata_id, policy_id, cpl_id, commodity_id, policy_element, start_date, end_date, units, value, value_text, value_type, exemptions, minavtariffvalue, notes, link, source, title_of_notice, legal_basis_name, date_of_publication, imposed_end_date, second_generation_specific, benchmark_tax, benchmark_product, tax_rate_biofuel, tax_rate_benchmark, start_date_tax, benchmark_link, original_dataset, type_of_change_code, type_of_change_name, measure_description, product_original_hs, product_original_name, link_pdf, benchmark_link_pdf, element_code, maxavtariffvalue, countavtariff, countnavtariff from "+step2TableName+" where cpl_id is not null");
            //Remove existing cpl from step 2 table and assign new cpl id to remaining rows
            connection.createStatement().executeUpdate("delete from "+step2TableName+" where cpl_id is not null");
            connection.createStatement().executeUpdate("select setval ('"+cplSequenceName+"', max(cpl_id)) from mastertable");
            connection.createStatement().executeUpdate("update "+step2TableName+" set cpl_id = nextval ('"+cplSequenceName+"')");
            //finish policysource table update
            statement = connection.prepareStatement("insert into policysource select cpl_id, policy_id, ? from "+step2TableName);
            statement.setString(1, source);
            statement.executeUpdate();
            //Finish policytable table update
            connection.createStatement().executeUpdate("insert into policytable select metadata_id, policy_id, cpl_id, commodity_id, policy_element, start_date, end_date, units, value, value_text, value_type, exemptions, minavtariffvalue, notes, link, source, title_of_notice, legal_basis_name, date_of_publication, imposed_end_date, second_generation_specific, benchmark_tax, benchmark_product, tax_rate_biofuel, tax_rate_benchmark, start_date_tax, benchmark_link, original_dataset, type_of_change_code, type_of_change_name, measure_description, product_original_hs, product_original_name, link_pdf, benchmark_link_pdf, element_code, maxavtariffvalue, countavtariff, countnavtariff from "+step2TableName);
            //Update mastertable table
            connection.createStatement().executeUpdate("insert into mastertable select cpl_id, commodity_id, country_code, country_name, subnational_code, subnational_name, commoditydomain_code, commoditydomain_name, commodityclass_code, commodityclass_name, policydomain_code, policydomain_name, policytype_code, policytype_name, policymeasure_code, policymeasure_name, condition_code, condition, individualpolicy_code, individualpolicy_name from "+step2TableName);
            //Update mastertableb table
            connection.createStatement().executeUpdate("insert into mastertableb select distinct cpl_id, subnational_code from "+step2TableName);
            //Reset metadata_id_linked_to_cpl_id table
            connection.createStatement().executeUpdate("truncate table metadata_id_linked_to_cpl_id");
            connection.createStatement().executeUpdate("insert into metadata_id_linked_to_cpl_id select distinct metadata_id, cpl_id, policy_id from policytable");
            //Refresh views
            connection.createStatement().executeUpdate("CREATE OR REPLACE VIEW policyTableViewPd_and_startDate AS select metadata_id, policy_id, policytable.cpl_id, country_code, country_name, commodityclass_code, commodityclass_name, policytype_code, policytype_name, policymeasure_code, policymeasure_name, commodity_id, policy_element, CASE WHEN value_text='elim' THEN lead(start_date) OVER (ORDER BY policytable.cpl_id ASC, date_of_publication DESC, start_date ASC) ELSE start_date END AS start_date, CASE WHEN value_text='elim' THEN start_date ELSE end_date END AS end_date, units, value, value_text, value_type, exemptions, minavtariffvalue, notes, link, source, title_of_notice, legal_basis_name, date_of_publication, imposed_end_date, second_generation_specific, benchmark_tax, benchmark_product, tax_rate_biofuel, tax_rate_benchmark, start_date_tax, benchmark_link, original_dataset, type_of_change_code, type_of_change_name, measure_description, product_original_hs, product_original_name, link_pdf, benchmark_link_pdf, element_code, maxavtariffvalue, countavtariff, countnavtariff from policytable JOIN (select cpl_id, country_code, country_name, commodityclass_code, commodityclass_name, policytype_code, policytype_name, policymeasure_code, policymeasure_name from mastertable)master ON master.cpl_id = policytable.cpl_id ORDER BY master.cpl_id ASC, date_of_publication DESC, start_date ASC");
            connection.createStatement().executeUpdate("TRUNCATE TABLE appForPolicyTableViewTest");
            connection.createStatement().executeUpdate("INSERT INTO appForPolicyTableViewTest SELECT metadata_id, policy_id, mastertable.cpl_id, country_code, country_name, commodityclass_code, commodityclass_name, policytype_code, policytype_name, policymeasure_code, policymeasure_name, mastertable.commodity_id, policy_element, start_date, end_date, units, value, value_text, value_type, exemptions, minavtariffvalue, notes, link, source, title_of_notice, legal_basis_name, date_of_publication, imposed_end_date, second_generation_specific, benchmark_tax, benchmark_product, tax_rate_biofuel, tax_rate_benchmark, start_date_tax, benchmark_link, original_dataset, type_of_change_code, type_of_change_name, measure_description, product_original_hs, product_original_name, link_pdf, benchmark_link_pdf, element_code, maxavtariffvalue, countavtariff, countnavtariff FROM mastertable, policytable where mastertable.cpl_id= policytable.cpl_id");
            connection.createStatement().executeUpdate("CREATE OR REPLACE view policyTableViewTest AS (SELECT * FROM appForPolicyTableViewTest)");
        } finally {
            //remove step 2 table in case error occur
            connection.createStatement().executeUpdate("drop table " + step2TableName);
        }
    }


    //Utils
    SimpleDateFormat timeSuffixFormat = new SimpleDateFormat("yyyyMMddhhmmss");
    private String getTimeSuffix(Date... date) {
        return timeSuffixFormat.format(date!=null && date.length>0 ? date[0] : new Date());
    }

}
