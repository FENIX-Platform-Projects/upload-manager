package org.fao.ess.uploader.oecd.policy.bulk.metadata.impl;

import java.io.*;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.xssf.eventusermodel.XLSX2CSV;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class XLStoCSV {

    public Collection<String[]> toCSV(InputStream input) throws Exception {
        //Out
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        //Parse Excel input
        OPCPackage p = OPCPackage.open(input);
        XLSX2CSV xlsx2csv = new XLSX2CSV(p, new PrintStream(outputStream), -1);
        xlsx2csv.process();
        p.close();
        //Parse CSV input
        String content = outputStream.toString();
        CSVFormat format = CSVFormat.EXCEL.withDelimiter(',').withIgnoreEmptyLines().withQuote('"').withEscape('\\');
        Collection<String[]> data = new LinkedList<>();
        for (CSVRecord record : CSVParser.parse(content, format)) {
            Collection<String> row = new LinkedList<>();
            for (Iterator<String> cellIterator = record.iterator() ; cellIterator.hasNext(); row.add(cellIterator.next()));
            if (row.size()>1)
                data.add(row.toArray(new String[row.size()]));
        }
        return data;
    }

}




/*
    public static void main(String[] args) throws Exception {
        FileInputStream input = new FileInputStream("test/Metadatafile_22Apr2016.xlsx");
        for (String[] row : new XLStoCSV().toCSV(input)) {
            for (String cell : row)
                System.out.print(cell+',');
            System.out.println();
        }
    }
    */
