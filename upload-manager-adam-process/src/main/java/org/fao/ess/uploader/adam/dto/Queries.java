package org.fao.ess.uploader.adam.dto;


public enum Queries {


    //CLEAN
    cleanDonorsGNI("TRUNCATE TABLE donors_gni"),
    cleanAdamCountryIndicator("TRUNCATE TABLE country_indicators"),
    cleanCPFPriorities("TRUNCATE TABLE fao_cpf_priorities"),
    cleanRecipientUndafPriorities("TRUNCATE TABLE recipient_undaf_priorities"),

    insertFoodGroups("INSERT INTO FOOD_GROUP(GROUP_CODE,SUBGROUP_CODE,FOODEX2_CODE) VALUES (?,?,?)"),

    //VALIDATE
    getUnexistingFoodGroup("SELECT DISTINCT CONSUMPTION_RAW.FOODEX2_CODE FROM CONSUMPTION_RAW LEFT JOIN FOOD_GROUP ON (CONSUMPTION_RAW.FOODEX2_CODE = FOOD_GROUP.FOODEX2_CODE) WHERE SUBGROUP_CODE IS NULL"),
    getSurveyList("SELECT DISTINCT SURVEY_CODE FROM (SELECT DISTINCT SURVEY_CODE FROM CONSUMPTION_RAW UNION ALL SELECT DISTINCT SURVEY_CODE FROM SUBJECT_RAW) SURVEYS"),

    //PUBLISH
    updateSubject("{ call refresh_subject(?) }"),
    updateConsumption("{ call refresh_consumption(?) }"),
//    updateMaster(""),
//    updateMasterAvg(""),

    ;

    private String query;

    Queries(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }
}
