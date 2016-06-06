package org.fao.ess.uploader.oecd.policy.bulk.utils;

import com.jcraft.jsch.*;
import org.fao.ess.uploader.core.init.UploaderConfig;
import org.fao.fenix.commons.utils.FileUtils;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.*;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;

@ApplicationScoped
public class FileManager {
    @Inject private UploaderConfig config;
    @Inject private FileUtils fileUtils;
    private File tmpFolder;
    private HostProperties properties;


    //SFTP operations management
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

    public void uploadAttachments(String source, String[] policyIDs) throws Exception {
        ChannelSftp channel = getConnection();
        File attachmentsFolder = getAttachmentsFolder(source);
        if (attachmentsFolder.exists() && attachmentsFolder.isDirectory())
            try {
                String destinationPath = properties.getPath() + '/' + source;
                mkDir(destinationPath, channel);
                channel.cd(destinationPath);

                for (File sourceFolder : attachmentsFolder.listFiles())
                    if (sourceFolder.isDirectory()) {
                        String policyID = null;
                        try { policyID = policyIDs[Integer.parseInt(sourceFolder.getName()) - 1]; } catch (Exception ex) { }
                        if (policyID!=null) {
                            String fileDestinationPath = destinationPath+'/'+policyID;
                            mkDir(fileDestinationPath, channel);
                            channel.cd(fileDestinationPath);

                            for (File sourceFile : sourceFolder.listFiles())
                                if (sourceFile.isFile())
                                    uploadAttachment(channel, sourceFile);
                        }
                    }
            } finally {
                if (channel!=null)
                    close(channel);
            }
    }

    public void backupAttachments(String source) throws Exception {
 /*       ChannelSftp channel = getConnection();
        try {
            String destinationPath = properties.getPath() + '/' + source;
            mkDir(destinationPath, channel);
            String backupPath = properties.getPath() + "/backup/" + source;
            mkDir(destinationPath, channel);
            channel.cd(backupPath);
/
            for (File sourceFolder : attachmentsFolder.listFiles())
                if (sourceFolder.isDirectory()) {
                    String policyID = null;
                    try { policyID = policyIDs[Integer.parseInt(sourceFolder.getName()) - 1]; } catch (Exception ex) { }
                    if (policyID!=null) {
                        String fileDestinationPath = destinationPath+'/'+policyID;
                        mkDir(fileDestinationPath, channel);
                        channel.cd(fileDestinationPath);

                        for (File sourceFile : sourceFolder.listFiles())
                            if (sourceFile.isFile())
                                uploadAttachment(channel, sourceFile);
                    }
                }
        } finally {
            if (channel!=null)
                close(channel);
        }
        */
    }

    public void restoreAttachments(String source) throws Exception {

    }


    private ChannelSftp getConnection() throws Exception {
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");

        JSch jsch = new JSch();
        Session session = jsch.getSession(properties.getUser(), properties.getHost(), properties.getPort());
        session.setPassword(properties.getPassword());
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

    private void mkDir(String path, ChannelSftp channel) throws SftpException {
        SftpATTRS attributes=null;
        try { attributes = channel.stat(path); } catch (Exception e) { }
        if (attributes == null)
            channel.mkdir(path);
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



    //Utils

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

}
