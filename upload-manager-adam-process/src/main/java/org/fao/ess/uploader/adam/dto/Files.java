package org.fao.ess.uploader.adam.dto;

public enum Files {

    cpf("cpf_priorities.csv"), undaf("undaf_priorities.csv"), country_indicators("country_indicators.csv"), donors_gni("donors_gni.csv") ;

    private String fileName;

    Files(String fileName) {
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }

    public static Files get(String fileName) {
        for (Files f : Files.values())
            if (f.getFileName().equalsIgnoreCase(fileName))
                return f;
        return null;
    }
}
