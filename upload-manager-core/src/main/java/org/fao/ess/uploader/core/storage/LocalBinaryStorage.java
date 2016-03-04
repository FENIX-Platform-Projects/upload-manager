package org.fao.ess.uploader.core.storage;

import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.init.UploaderConfig;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.*;
import java.util.Arrays;
import java.util.Comparator;

@ApplicationScoped
public class LocalBinaryStorage extends BinaryStorage {
    private @Inject UploaderConfig config;

    private File folder;


    //INTERFACE

    @Override
    public void init() throws Exception {
        //Load init properties and init folder
        String path = config.get("storage.path");
        folder = path!=null ? new File(path) : new File("/tmp/upload_manager");
        if (!folder.exists())
            folder.mkdirs();
    }

    @Override
    public void writeChunk(ChunkMetadata chunkMetadata, InputStream input) throws Exception {
        File file = new File(folder,getFileName(chunkMetadata));
        if (checkChunk(chunkMetadata))
            file.delete();
        appendToFile(file,input);
    }

    @Override
    public InputStream readFile(FileMetadata fileMetadata, OutputStream output) throws Exception {
        try {
            InputStream input = new FileInputStream(new File(folder,getFileName(fileMetadata)));

            if (output != null) {
                copy(output, input);
                return null;
            } else
                return input;
        } finally {
            if (output!=null)
                output.close();
        }
    }

    @Override
    public void closeFile(FileMetadata fileMetadata) throws Exception {
        File output = new File(folder,getFileName(fileMetadata));
        for (String chunkFileName : chunksFile(fileMetadata))
            appendToFile(output,new FileInputStream(new File(folder,chunkFileName)));
    }

    @Override
    public void removeFile(FileMetadata fileMetadata) throws Exception {
        new File(folder,getFileName(fileMetadata)).delete();
    }

    @Override
    public void removeChunk(ChunkMetadata chunkMetadata) throws Exception {
        new File(folder,getFileName(chunkMetadata)).delete();
    }

    //LOGIC

    private void appendToFile (File file, InputStream input) throws Exception {
        OutputStream output = new FileOutputStream(file,true);
        copy(output,input);
        output.flush();
        output.close();
    }
    private void copy (OutputStream output, InputStream input) throws Exception {
        byte[] buffer = new byte[1024];
        for (int c = input.read(buffer); c>0; c = input.read(buffer))
            output.write(buffer,0,c);
    }



    //UTILS

    //Retrieve file name
    private String getFileName(FileMetadata fileMetadata) {
        StringBuilder name = new StringBuilder();
        if (fileMetadata!=null) {
            if (fileMetadata.getContext()!=null)
                name.append(fileMetadata.getContext()).append("___");
            name.append(fileMetadata.getMd5());
        }
        return name.length()>0 ? name.toString() : null;
    }

    //Retrive chunk file name
    private String getFileName(ChunkMetadata chunkMetadata) {
        if (chunkMetadata==null)
            return null;
        String baseFileName = getFileName(chunkMetadata.getFile());
        return baseFileName!=null ? baseFileName+"___"+chunkMetadata.getIndex() : null;
    }

    //List existing chunks for a specific file
    private String[] chunksFile (FileMetadata fileMetadata) throws Exception {
        final String prefix = getFileName(fileMetadata)+"___";
        final int prefixLength = prefix.length();

        File[] chunksFile = prefix == null ? new File[0] :
            folder.listFiles(new FileFilter() {
                @Override
                public boolean accept(File file) {
                    return file.getName().startsWith(prefix);
                }
            });

        String[] chunksFileName = new String[chunksFile.length];
        for (int i=0; i<chunksFile.length; i++)
            chunksFileName[i] = chunksFile[i].getName();
        Arrays.sort(chunksFileName, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return new Integer(o1.substring(prefixLength)).compareTo(new Integer(o2.substring(prefixLength)));
            }
        });

        return chunksFileName;
    }

    //Check chunk file availability
    private boolean checkChunk (ChunkMetadata chunkMetadata) throws Exception {
        final String name = getFileName(chunkMetadata);
        File[] files = folder.listFiles(new FileFilter() {
            public boolean accept(File file) {
                return file.getName().equals(name);
            }
        });

        return files!=null && files.length==1;
    }


}
