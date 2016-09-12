package org.fao.ess.uploader.gift.bulk.dto;

public enum Queries {

    //CLEAN
    cleanFoodGroups("TRUNCATE TABLE FOOD_GROUP"),
    cleanSubjectRaw("TRUNCATE TABLE SUBJECT_RAW"),
    cleanConsumptionRaw("TRUNCATE TABLE CONSUMPTION_RAW"),
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
