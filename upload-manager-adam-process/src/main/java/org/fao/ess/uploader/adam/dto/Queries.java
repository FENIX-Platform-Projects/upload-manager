package org.fao.ess.uploader.adam.dto;


public enum Queries {

    //CLEAN
    cleanDonorsGNI("TRUNCATE TABLE donors_gni"),
    cleanAdamCountryIndicator("TRUNCATE TABLE country_indicators"),

    //CREATE TMP TABLES
    createCPFPriorites("CREATE TEMP TABLE fao_cpf_priorities\n" +
            "(\n" +
            "  recipientcode text,\n" +
            "  recipientname text,\n" +
            "  from_year text,\n" +
            "  to_year text,\n" +
            "  stated_priority text,\n" +
            "  purposecode text,\n" +
            "  purposename text\n" +
            ")"),

    createUndafPriorites("CREATE TEMP TABLE recipient_undaf_priorities\n" +
            "(\n" +
            "  recipientcode text,\n" +
            "  recipientname text,\n" +
            "  from_year text,\n" +
            "  to_year text,\n" +
            "  stated_priority text,\n" +
            "  purposecode text,\n" +
            "  purposename text\n" +
            ")"),

    //FUNCTIONS
    callFunctionCPFUndaf("{ call create_cpf_undaf_priorities_table() }"),
    callFunctionPrioritiesTable("{ call create_priorities_table() }");

    private String query;

    Queries(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }
}
