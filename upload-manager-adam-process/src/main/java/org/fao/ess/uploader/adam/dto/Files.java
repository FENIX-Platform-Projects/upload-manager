package org.fao.ess.uploader.adam.dto;

public enum Files {

    cpf("cpf_priorities.csv"), undaf("undaf_priorities.csv");
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
