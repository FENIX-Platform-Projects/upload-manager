package org.fao.ess.uploader.core.storage.impl;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPSClient;
import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.dto.Status;
import org.fao.ess.uploader.core.init.UploaderConfig;
import org.fao.ess.uploader.core.storage.BinaryStorage;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.net.Inet4Address;
import java.net.InetAddress;

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
    public Status writeChunk(ChunkMetadata chunkMetadata, InputStream input) throws Exception {
        open();
        return null;
    }

    @Override
    public Status closeFile(FileMetadata fileMetadata) throws Exception {
        return null;
    }

    @Override
    public Status checkStatus(String fileName) throws Exception {
        return null;
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


    //UTILS


}
