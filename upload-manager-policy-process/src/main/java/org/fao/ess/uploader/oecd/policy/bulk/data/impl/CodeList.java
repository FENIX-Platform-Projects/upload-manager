package org.fao.ess.uploader.oecd.policy.bulk.data.impl;

public enum CodeList {

    PolicyType ("OECD_PolicyType","1.0", "select distinct policytype_code, policytype_name from mastertable"),
    IndividualPolicy ("OECD_IndividualPolicy","1.0", "select distinct individualpolicy_code, individualpolicy_name from mastertable order by individualpolicy_code"),
    Element ("OECD_Element","1.0", "select distinct element_code, policy_element from policytable order by element_code"),
    Condition ("OECD_Condition","1.0", "select distinct condition_code, condition from mastertable order by condition_code"),
    TypeOfChange ("OECD_TypeOfChange","1.0", "select distinct type_of_change_code, type_of_change_name from policytable order by type_of_change_code"),
    ;

    private String query;
    private String uid;
    private String version;
    CodeList(String uid, String version, String query) {
        this.query = query;
        this.uid = uid;
        this.version = version;
    }

    public String getQuery() {
        return query;
    }

    public String getUid() {
        return uid;
    }

    public String getVersion() {
        return version;
    }
}
