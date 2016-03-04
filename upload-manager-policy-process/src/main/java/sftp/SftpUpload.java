package sftp;

import java.io.*;
import java.lang.*;
import java.util.Properties;

import com.jcraft.jsch.*;

public class SftpUpload {

    public SftpUpload() {
    }

    public void connect(Properties prop, Session session, Channel channel, ChannelSftp channelSftp, File f, String policyId) throws Exception {
        connect(prop, session, channel, channelSftp, f!=null ? new FileInputStream(f) : null, f!=null ? f.getName() : null, policyId);
    }
    public void connect(Properties prop, Session session, Channel channel, ChannelSftp channelSftp, InputStream fileStream, String fileName, String policyId) throws Exception {
        String SFTPHOST = prop.getProperty("SFTPHOST");
        String SFTPPORT = prop.getProperty("SFTPPORT");
        String SFTPUSER = prop.getProperty("SFTPUSER");
        String SFTPPASS = prop.getProperty("SFTPPASS");

        JSch jsch = new JSch();
        if((SFTPHOST!=null)&&(SFTPPORT!=null)&&(SFTPUSER!=null)&&(SFTPPASS!=null)){
            session = jsch.getSession(SFTPUSER,SFTPHOST, Integer.parseInt(SFTPPORT));
            session.setPassword(SFTPPASS);
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect();
            System.out.println("Before upload...");
            uploadFile(prop, session, channel, channelSftp, fileStream, fileName, policyId);
        }
        else {
            String err = prop.getProperty("ERROR_PARAMETERS");
            throw new Exception(err);
        }
    }

    public void uploadFile(Properties prop, Session session, Channel channel, ChannelSftp channelSftp, File file, String policyId) throws Exception {
        String FILENAME = file!=null && file.isFile() ? file.getName() : null;
        InputStream fileInputStream = file!=null && file.isFile() ? new FileInputStream(file) : null;

        uploadFile(prop, session, channel, channelSftp, fileInputStream, FILENAME, policyId);
    }
    public void uploadFile(Properties prop, Session session, Channel channel, ChannelSftp channelSftp, InputStream fileStream, String fileName, String policyId) throws Exception {

        String SFTPSERVERDIR = prop.getProperty("SFTPSERVERDIR");

        if( fileStream!=null && fileName!=null && policyId!=null ){

            channel = session.openChannel("sftp");
            channel.connect();
            channelSftp = (ChannelSftp) channel;

            String newUploadDirectory = SFTPSERVERDIR + "/" + policyId;
            uploadDirectoryCreation(newUploadDirectory, channelSftp);
            channelSftp.cd(newUploadDirectory);

            //Qui collegare il file messo in input
            SftpProgressMonitor monitor= new CustomProgressMonitor(fileName, policyId);
            System.out.println("Before sending file....");
            channelSftp.put(fileStream, fileName, monitor, ChannelSftp.OVERWRITE);
            channelSftp.disconnect();
        }
        else {
            System.out.println("Before disconnect...");
            disconnect(session);
            String err = prop.getProperty("ERROR_PARAMETERS");
            throw new Exception(err);
        }

        System.out.println("Before disconnect...");
        disconnect(session);
    }

    boolean uploadDirectoryCreation(String path, ChannelSftp channelSftp) throws SftpException {
        SftpATTRS attrs=null;

        try {
            attrs = channelSftp.stat(path);
        } catch (Exception e) {
            System.out.println(path+" not found");
        }

        if (attrs != null) {
            System.out.println("Directory exists IsDir="+attrs.isDir());
        } else {
            System.out.println("Creating dir "+path);
            channelSftp.mkdir(path);
        }

        return true;
    }

    public void disconnect(Session session){
        session.disconnect();
    }

}
//SUBSCRIBE/FOLLOW US
//        - See more at: http://kodehelp.com/java-program-for-uploading-file-to-sftp-server/#sthash.pLYM6gKp.dpuf
//http://www.programcreek.com/java-api-examples/index.php?api=com.jcraft.jsch.ChannelSftp

//https://docs.oracle.com/javaee/6/tutorial/doc/giplj.html#gipln

