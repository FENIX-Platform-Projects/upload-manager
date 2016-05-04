package metadata.bulk.excel;

import java.io.*;
import java.util.Iterator;

import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.xssf.eventusermodel.XLSX2CSV;
import org.fao.fenix.commons.utils.CSVReader;

public class XLStoCSV {
    public Iterable<String[]> toCSV(InputStream input) throws Exception {
        //Out
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        //Parse Excel input
        OPCPackage p = OPCPackage.open(input);
        XLSX2CSV xlsx2csv = new XLSX2CSV(p, new PrintStream(outputStream), -1);
        xlsx2csv.process();
        p.close();
        //Parse CSV input
        byte[] content = outputStream.toByteArray();
        Iterable<String[]> csv = new CSVReader(new ByteArrayInputStream(content),",");
        //Skip first 2 rows (they contains excel sheet information)
        Iterator<String[]> csvIterator = csv.iterator();
        csvIterator.next();
        csvIterator.next();
        //Return csv
        return csv;
    }


    public static void main(String[] args) throws Exception {
        FileInputStream input = new FileInputStream("test/Metadatafile_22Apr2016.xlsx");
        for (String[] row : new XLStoCSV().toCSV(input)) {
            for (String cell : row)
                System.out.print(cell+',');
            System.out.println();
        }
    }
}