package org.fao.ess.uploader.gift.bulk.impl;

import org.fao.ess.uploader.core.init.UploaderConfig;
import org.fao.ess.uploader.gift.bulk.dto.Files;
import org.fao.fenix.commons.utils.FileUtils;
import org.fao.fenix.commons.utils.UIDUtils;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.*;
import java.util.HashMap;
import java.util.Map;


@ApplicationScoped
public class FileManager {
    @Inject private UploaderConfig config;
    @Inject private FileUtils fileUtils;
    @Inject private UIDUtils uidUtils;
    private File tmpFolder;

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

    public void removeTmpFolder(File folder) throws Exception {
        fileUtils.delete(folder);
    }


    public File saveFile(File tmpFolder, String surveyCode, InputStream zipFileInput) throws IOException {
        File destinationFile = new File(tmpFolder, "survey_"+surveyCode+".zip");
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
}
