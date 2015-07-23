package org.fao.ess.uploader.core.storage.ftp;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPFileFilter;
import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.init.UploaderConfig;
import org.fao.ess.uploader.core.storage.BinaryStorage;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Comparator;

@ApplicationScoped
public class FTPBinaryStorage extends BinaryStorage {
    private @Inject UploaderConfig config;
    private FTPClient ftpClient;

    private InetAddress host;
    private Integer port;
    private String usr;
    private String psw;


    //INTERFACE

    @Override
    public void init() throws Exception {
        //Load init properties
        String hostName = config.get("storage.host");
        String hostAddress = config.get("storage.address");
        String hostPort = config.get("storage.port");
        usr = config.get("storage.usr");
        psw = config.get("storage.psw");
        //Retrieve host address
        byte[] hostAddressBytes = new byte[4];
        if (hostAddress!=null)
            try {
                String[] addressValues = hostAddress.split("\\.");
                for (int i = 0; i<addressValues.length; i++)
                    if ((hostAddressBytes[i] = Byte.parseByte(addressValues[i])) < 0)
                        throw new Exception();
            } catch (Exception ex) {
                throw new Exception("Wrong FTP storage host IP address format: '"+hostAddress+'\'');
            }
        if (hostName!=null)
            if (hostAddress!=null)
                host = InetAddress.getByAddress(hostName, hostAddressBytes);
            else
                host = InetAddress.getByName(hostName);
        else if (hostAddress!=null)
            host = InetAddress.getByAddress(hostAddressBytes);
        if (host==null)
            throw new Exception("No FTP storage host address specified");
        //Retrieve host port
        try {
            port = hostPort != null ? Integer.parseInt(hostPort) : null;
            if (port!=null && port<0)
                throw new Exception();
        } catch (NumberFormatException ex) {
            throw new Exception("Wrong FTP storage host port format: '"+hostPort+'\'');
        }
        //Create FTP client
        ftpClient = new FTPClient();
    }

    @Override
    public void writeChunk(ChunkMetadata chunkMetadata, InputStream input) throws Exception {
        open();
        try {
            if (checkChunk(chunkMetadata))
                ftpClient.deleteFile(getFileName(chunkMetadata));
            ftpClient.appendFile(getFileName(chunkMetadata), input);
        } finally {
            input.close();
        }
    }

    @Override
    public InputStream readFile(FileMetadata fileMetadata, OutputStream outputStream) throws Exception {
        open();
        if (outputStream!=null)
            try {
                ftpClient.retrieveFile(getFileName(fileMetadata),outputStream);
                return null;
            } finally {
                outputStream.close();
            }
        else
            return ftpClient.retrieveFileStream(getFileName(fileMetadata));
    }

    @Override
    public void closeFile(FileMetadata fileMetadata) throws Exception {
        open();
        OutputStream output = ftpClient.appendFileStream(getFileName(fileMetadata));
        try {
            for (String chunkFileName : chunksFile(fileMetadata))
                ftpClient.retrieveFile(chunkFileName,output);
        } finally {
            output.close();
        }
    }

    @Override
    public void removeFile(FileMetadata fileMetadata) throws Exception {
        open();
        ftpClient.deleteFile(getFileName(fileMetadata));
    }

    @Override
    public void removeChunk(ChunkMetadata chunkMetadata) throws Exception {
        open();
        ftpClient.deleteFile(getFileName(chunkMetadata));
    }

    //INTERNAL LOGIC

    //Connection management
    private synchronized void open() throws IOException {
        if (ftpClient!=null && ftpClient.isAvailable() && !ftpClient.isConnected()) {
            if (port!=null)
                ftpClient.connect(host,port);
            else
                ftpClient.connect(host);
            if (usr!=null)
                ftpClient.login(usr, psw);
        }
    }
    private void close() throws IOException {
        if (ftpClient!=null && ftpClient.isAvailable() && ftpClient.isConnected())
            ftpClient.disconnect();
    }

    //Chunks read
    private boolean checkChunk (ChunkMetadata chunkMetadata) throws Exception {
        FTPFile[] files = ftpClient.listFiles(getFileName(chunkMetadata));
        return files!=null && files.length==1;
    }




    //UTILS

    private String getFileName(FileMetadata fileMetadata) {
        StringBuilder name = new StringBuilder("./");
        if (fileMetadata!=null) {
            if (fileMetadata.getContext()!=null)
                name.append(fileMetadata.getContext()).append('/');
            name.append(fileMetadata.getName()!=null ? fileMetadata.getName() : fileMetadata.getMd5());
        }
        return name.length()>2 ? name.toString() : null;
    }

    private String getFileName(ChunkMetadata chunkMetadata) {
        if (chunkMetadata==null)
            return null;
        String baseFileName = getFileName(chunkMetadata.getFile());
        return baseFileName!=null ? baseFileName+"___"+chunkMetadata.getIndex() : null;
    }

    private String[] chunksFile (FileMetadata fileMetadata) throws Exception {
        String folder = fileMetadata!=null ? "./"+(fileMetadata.getContext()!=null ? fileMetadata.getContext() : "") : null;
        String name = folder!=null ? (fileMetadata.getName()!=null ? fileMetadata.getName() : fileMetadata.getMd5()) : null;
        final String prefix = name!=null ? name+"___" : null;

        FTPFile[] chunksFile = prefix == null ? null :
            ftpClient.listFiles(folder, new FTPFileFilter() {
                @Override
                public boolean accept(FTPFile ftpFile) {
                    return ftpFile.getName().startsWith(prefix);
                }
            });
        chunksFile = chunksFile==null ? new FTPFile[0] : chunksFile;
        Arrays.sort(chunksFile, new Comparator<FTPFile>() {
            @Override
            public int compare(FTPFile o1, FTPFile o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });

        if (!folder.endsWith("/"))
            folder = folder + '/';
        String[] chunksFileName = new String[chunksFile.length];
        for (int i=0; i<chunksFile.length; i++)
            chunksFileName[i] = folder+chunksFile[i].getName();

        return chunksFileName;
    }



}
