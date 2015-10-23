package org.fao.ess.uploader.rlm.processor;

import java.io.*;
import java.util.Collection;
import java.util.LinkedList;

public class RLMCSVParser {
    private BufferedReader input;
    boolean closed = false;
    boolean title = true;

    public RLMCSVParser(InputStream inputStream) throws Exception {
        input = new BufferedReader(new InputStreamReader(inputStream));
    }

    public String[] nextRow() throws IOException {
        //Read next text line if file is open
        if (closed)
            return null;
        String line = input.readLine();
        //Close at the end of the file and set closed status
        if (line==null) {
            input.close();
            closed = true;
            return null;
        }
        //Ignore empty rows
        if (line.trim().length()==0)
            return nextRow();
        //Ignore title row
        if (title) {
            title = false;
            return nextRow();
        }

        //Row normalization
        Collection<String> row = new LinkedList<>();
        line = line.trim();
        while (line!=null) {
            int nextIndex;
            String item;
            if (line.trim().length()==0) {
                item = "";
                nextIndex = 0;
            } else if (line.charAt(0) == '"') {
                nextIndex = line.indexOf('"', 1);
                item = line.substring(1, nextIndex).trim();
                nextIndex = line.indexOf(',', nextIndex);
            } else {
                nextIndex = line.indexOf(',');
                item = nextIndex > 0 ? line.substring(0, nextIndex).trim() : line;
            }
            line = nextIndex > 0 ? line.substring(nextIndex + 1) : null;
            row.add(item.length()>0 ? item : null);
        }

        //Return row
        return row.toArray(new String[row.size()]);
    }

}
