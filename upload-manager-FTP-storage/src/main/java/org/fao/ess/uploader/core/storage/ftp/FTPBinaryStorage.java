package org.fao.ess.uploader.core.storage.ftp;

import org.apache.commons.net.ftp.FTP;
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

    private InetAddress host;
    private Integer port;
    private String usr;
    private String psw;
    private String path;


    //INTERFACE

    @Override
    public void init() throws Exception {
        //Load init properties
        String hostName = config.get("storage.host");
        String hostAddress = config.get("storage.address");
        String hostPort = config.get("storage.port");
        usr = config.get("storage.usr");
        psw = config.get("storage.psw");
        path = config.get("storage.path");
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
    }

    @Override
    public void writeChunk(ChunkMetadata chunkMetadata, InputStream input) throws Exception {
        try {
            writeChunk(chunkMetadata, input, 2);
        } finally {
            input.close();
        }
    }

    @Override
    public InputStream readFile(FileMetadata fileMetadata, OutputStream outputStream) throws Exception {
        try {
            return readFile(fileMetadata, outputStream, 2);
        } finally {
            if (outputStream!=null)
                outputStream.close();
        }
    }

    @Override
    public void closeFile(FileMetadata fileMetadata) throws Exception {
        closeFile(fileMetadata, 2);
    }

    @Override
    public void removeFile(FileMetadata fileMetadata) throws Exception {
        removeFile(fileMetadata, 2);
    }

    @Override
    public void removeChunk(ChunkMetadata chunkMetadata) throws Exception {
        removeChunk(chunkMetadata, 2);
    }

    //LOGIC

    private void writeChunk(ChunkMetadata chunkMetadata, InputStream input, int remainingAttempts) throws Exception {
        FTPClient ftpClient = connect();
        try {
            if (checkChunk(ftpClient, chunkMetadata))
                ftpClient.deleteFile(getFileName(chunkMetadata));
            ftpClient.appendFile(getFileName(chunkMetadata), input);
        } catch (Exception ex) {
            if (--remainingAttempts>0)
                writeChunk(chunkMetadata,input,remainingAttempts);
            else
                throw ex;
        } finally {
            try { ftpClient.disconnect(); } catch (Exception ex) {}
        }
    }

    private InputStream readFile(FileMetadata fileMetadata, OutputStream outputStream, int remainingAttempts) throws Exception {
        FTPClient ftpClient = connect();
        try {
            if (outputStream != null) {
                ftpClient.retrieveFile(getFileName(fileMetadata), outputStream);
                return null;
            } else
                return ftpClient.retrieveFileStream(getFileName(fileMetadata));
        } catch (Exception ex) {
            if (--remainingAttempts>0)
                return readFile(fileMetadata, outputStream, remainingAttempts);
            else
                throw ex;
        } finally {
            try { ftpClient.disconnect(); } catch (Exception ex) {}
        }
    }

    private void closeFile(FileMetadata fileMetadata, int remainingAttempts) throws Exception {
        FTPClient ftpInputClient = connect();
        FTPClient ftpOutputClient = connect();
        OutputStream output = ftpOutputClient.appendFileStream(getFileName(fileMetadata));
        try {
            for (String chunkFileName : chunksFile(ftpInputClient, fileMetadata))
                ftpInputClient.retrieveFile(chunkFileName,output);
        } catch (Exception ex) {
            if (--remainingAttempts>0)
                closeFile(fileMetadata, remainingAttempts);
            else
                throw ex;
        } finally {
            output.close();
            try { ftpInputClient.disconnect(); } catch (Exception ex) {}
            try { ftpOutputClient.disconnect(); } catch (Exception ex) {}
        }
    }

    private void removeFile(FileMetadata fileMetadata, int remainingAttempts) throws Exception {
        FTPClient ftpClient = connect();
        try {
            ftpClient.deleteFile(getFileName(fileMetadata));
        } catch (Exception ex) {
            if (--remainingAttempts>0)
                removeFile(fileMetadata,remainingAttempts);
            else
                throw ex;
        } finally {
            try { ftpClient.disconnect(); } catch (Exception ex) {}
        }
    }

    private void removeChunk(ChunkMetadata chunkMetadata, int remainingAttempts) throws Exception {
        FTPClient ftpClient = connect();
        try {
            ftpClient.deleteFile(getFileName(chunkMetadata));
        } catch (Exception ex) {
            if (--remainingAttempts>0)
                removeChunk(chunkMetadata,remainingAttempts);
            else
                throw ex;
        } finally {
            try { ftpClient.disconnect(); } catch (Exception ex) {}
        }
    }



    //UTILS

    //Retrieve connection
    private FTPClient connect() throws IOException {
        FTPClient ftpClient = new FTPClient();

        if (port!=null)
            ftpClient.connect(host, port);
        else
            ftpClient.connect(host);
        if (usr!=null)
            ftpClient.login(usr, psw);

        if (path!=null)
            ftpClient.changeWorkingDirectory(path);

        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

        return ftpClient;
    }

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
    private String[] chunksFile (FTPClient ftpClient, FileMetadata fileMetadata) throws Exception {
        final String prefix = getFileName(fileMetadata)+"___";
        final int prefixLength = prefix.length();

        FTPFile[] chunksFile = prefix == null ? new FTPFile[0] :
            ftpClient.listFiles("./", new FTPFileFilter() {
                @Override
                public boolean accept(FTPFile ftpFile) {
                    return ftpFile.getName().startsWith(prefix);
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
    private boolean checkChunk (FTPClient ftpClient, ChunkMetadata chunkMetadata) throws Exception {
        final String name = getFileName(chunkMetadata);
        FTPFile[] files = ftpClient.listFiles("./", new FTPFileFilter() {
            @Override
            public boolean accept(FTPFile ftpFile) {
                return ftpFile.getName().equals(name);
            }
        });

        return files!=null && files.length==1;
    }


}
