package org.fao.ess.uploader.adam.impl;

import org.fao.ess.uploader.adam.dto.Files;
import org.fao.ess.uploader.core.init.UploaderConfig;
import org.fao.fenix.commons.utils.FileUtils;
import org.fao.fenix.commons.utils.UIDUtils;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;



@ApplicationScoped
public class FileManager {
    @Inject private UploaderConfig config;
    @Inject private FileUtils fileUtils;
    @Inject private UIDUtils uidUtils;
    private File tmpFolder;
    private static final String DEFAULT_DATE_FORMAT = "YYYY_MM_DD";


    //Temporary folder management
    public File createTmpFolder() throws IOException {
        //Create empty tmp folder
        if (tmpFolder==null) {
            String tmpFilePath = config.get("gift.local.folder.tmp");
            tmpFolder = new File(tmpFilePath != null ? tmpFilePath : "/tmp");
        }
        File destinationFolder = new File(tmpFolder, uidUtils.newId());
        destinationFolder.mkdirs();
        //return folder
        return destinationFolder;
    }

    public Map<Files, File> unzip(File folder, InputStream zipPackageStream) throws Exception {
        Map<Files, File> recognizedFilesMap = new HashMap<>();
        fileUtils.unzip(zipPackageStream, folder, true);
        for (File file : folder.listFiles()) {
            Files recognizedFile = file.isFile() ? Files.get(file.getName()) : null;
            if (recognizedFile!=null)
                recognizedFilesMap.put(recognizedFile, file);
        }
        return recognizedFilesMap;
    }

    public Map<Files, File> mapFiles(File file) throws Exception {
        Map<Files, File> recognizedFilesMap = new HashMap<>();
            Files recognizedFile = file.isFile() ? Files.get(file.getName()) : null;
            if (recognizedFile!=null)
                recognizedFilesMap.put(recognizedFile, file);

        return recognizedFilesMap;
    }

    public void removeTmpFolder(File folder) throws Exception {
        fileUtils.delete(folder);
    }


    public File saveFile(File tmpFolder, InputStream zipFileInput, String fileName, String extension) throws IOException {
        File destinationFile = new File(tmpFolder, fileName+"."+extension);
        OutputStream out = new FileOutputStream(destinationFile);

        byte[] buffer = new byte[1024];
        try {
            for (int c = zipFileInput.read(buffer); c > 0; c = zipFileInput.read(buffer))
                out.write(buffer, 0, c);
        } finally {
            out.close();
        }
        return destinationFile;
    }



    //Other utils
    private String textToFileName(String text) {
        return text!=null ? text.replaceAll("[^\\w\\d_\\-.àèéìòù]"," ").replaceAll(" +"," ").trim().replace(' ','_') : null;
    }

}
