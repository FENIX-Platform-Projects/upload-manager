package org.fao.ess.uploader.oecd.policy.bulk.attachments.impl;

import com.jcraft.jsch.*;
import org.fao.ess.uploader.core.init.UploaderConfig;
import org.fao.ess.uploader.oecd.policy.bulk.attachments.dto.HostProperties;
import org.fao.fenix.commons.utils.FileUtils;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

@ApplicationScoped
public class FileManager {
    @Inject private UploaderConfig config;
    @Inject private FileUtils fileUtils;
    private File tmpFolder;
    private HostProperties properties;


    //SFTP operations management
    public void uploadAttachments(String source, String[] policyIDs) throws Exception {
        ChannelSftp channel = getConnection();
        File attachmentsFolder = getAttachmentsFolder(source);
        if (attachmentsFolder.exists() && attachmentsFolder.isDirectory()) {
            //Create path to needed folders
            String backupPath = properties.getPath() + "/backup/" + source + '/' + getTimeSuffix();
            String destinationPath = properties.getPath() + '/' + source;
            String lastBackupPath = getLastBackupPath(properties.getPath() + "/backup/" + source, channel);
            try {
                //Create backup folder
                mkDir(backupPath, channel);
                try {
                    //Copy new files into the new backup folder
                    for (File sourceFolder : attachmentsFolder.listFiles())
                        if (sourceFolder.isDirectory()) {
                            String policyID = null;
                            try {
                                policyID = policyIDs[Integer.parseInt(sourceFolder.getName()) - 1];
                            } catch (Exception ex) {
                            }
                            if (policyID != null) {
                                String fileDestinationPath = backupPath + '/' + policyID;
                                mkDir(fileDestinationPath, channel);
                                channel.cd(fileDestinationPath);

                                for (File sourceFile : sourceFolder.listFiles())
                                    if (sourceFile.isFile())
                                        uploadAttachment(channel, sourceFile);
                            }
                        }
                    //Refresh link to the new backup folder
                    channel.rm(destinationPath);
                    try {
                        channel.symlink(destinationPath, backupPath);
                    } catch (Exception ex) {
                        channel.symlink(destinationPath, lastBackupPath); //try to restore the link to the previous backup folder
                        throw ex;
                    }
                } catch (Exception ex) {
                    rmDir(backupPath, channel); //try to roll back backup folder creation
                    throw ex;
                }
            } finally {
                if (channel != null && !channel.isClosed())
                    close(channel);
            }
        }
    }

    private void uploadAttachment(ChannelSftp channel, File sourceFile) throws Exception {
        uploadAttachment(channel, new FileInputStream(sourceFile), sourceFile.getName());
    }

    private void uploadAttachment(ChannelSftp channel, InputStream fileStream, String fileName) throws Exception {
        SftpProgressMonitor monitor= new SftpProgressMonitor() {
            private String s, s1;

            @Override
            public void init(int i, String s, String s1, long l) {
                System.out.println("sftp init: "+i+" - "+s+" - "+s1+" - "+l);
                this.s = s;
                this.s1 = s1;
            }

            @Override
            public boolean count(long l) {
                System.out.println("sftp count: "+l);
                return false;
            }

            @Override
            public void end() {
                System.out.println("sftp done: "+s+" - "+s1);
            }
        };
        channel.put(fileStream, fileName, monitor, ChannelSftp.OVERWRITE);
        channel.disconnect();
    }



    //Temporary folder management
    public File createTmpFolder(InputStream zipPackageStream, String source) throws IOException {
        File destinationFolder = getFolder(source);
        fileUtils.unzip(zipPackageStream, destinationFolder, true);

        File[] content = destinationFolder.listFiles();
        if (content.length==1 && content[0].isDirectory() && !content[0].getName().equalsIgnoreCase("attachments")) {
            for (File f : content[0].listFiles())
                fileUtils.copy(f, destinationFolder);
            fileUtils.delete(content[0]);
        }

        return destinationFolder;
    }

    public void removeTmpFolder (String source) {
        fileUtils.delete(getFolder(source));
    }

    public InputStream getMetadataFileStream(String source) throws FileNotFoundException {
        return getFile(source,"metadata.xlsx");
    }
    public InputStream getDataFileStream(String source) throws FileNotFoundException {
        return getFile(source,"data.csv");
    }
    public File getAttachmentsFolder(String source) throws FileNotFoundException {
        return getFolder(source,"attachments");
    }



    //File utils

    private File getFolder(String source) {
        if (tmpFolder==null) {
            String tmpFilePath = config.get("local.folder.tmp");
            tmpFolder = new File(tmpFilePath != null ? tmpFilePath : "/tmp");
        }

        File folder = new File(tmpFolder, source);
        if (!folder.exists())
            folder.mkdirs();
        return folder;
    }
    private File getFolder(String source, String path) throws FileNotFoundException {
        File folder = new File(getFolder(source), path);
        return folder.exists() && folder.isDirectory() ? folder : null;
    }
    private InputStream getFile(String source, String path) throws FileNotFoundException {
        File file = new File(getFolder(source), path);
        return file.exists() && file.isFile() ? new FileInputStream(file) : null;
    }


    //SFTP utils

    private void initSFTP() throws Exception {
        if (properties==null)
            properties = new HostProperties(
                    null,
                    config.get("remote.host"),
                    new Integer(config.get("remote.port")),
                    config.get("remote.usr"),
                    config.get("remote.psw"),
                    config.get("remote.path")
            );
    }

    private ChannelSftp getConnection() throws Exception {
        initSFTP();

        Session session = new JSch().getSession(properties.getUser(), properties.getHost(), properties.getPort());
        session.setPassword(properties.getPassword());
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);
        session.connect();

        ChannelSftp channel = (ChannelSftp)session.openChannel("sftp");
        channel.connect();
        return channel;
    }
    private void close(ChannelSftp channel) {
        try { channel.disconnect(); } catch (Exception ex) {}
        try { channel.getSession().disconnect(); } catch (Exception ex) {}
    }

    private void mkDir(String path, ChannelSftp channel) throws SftpException {
        SftpATTRS attributes=null;
        try { attributes = channel.stat(path); } catch (Exception e) { }
        if (attributes == null)
            channel.mkdir(path);
    }
    private void rmDir(String path, ChannelSftp channel) throws SftpException {
        SftpATTRS attributes=null;
        try { attributes = channel.stat(path); } catch (Exception e) { }
        if (attributes != null)
            channel.rmdir(path);
    }

    SimpleDateFormat timeSuffixFormat = new SimpleDateFormat("yyyyMMddhhmmss");
    private String getTimeSuffix(Date ... date) {
        return timeSuffixFormat.format(date!=null && date.length>0 ? date[0] : new Date());
    }

    private String getLastBackupPath(String path, ChannelSftp channel) throws SftpException {
        Vector<String> content = channel.ls(path);
        Collections.sort(content);
        return content.lastElement();
    }
}
