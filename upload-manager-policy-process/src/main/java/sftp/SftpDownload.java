package sftp;
import java.io.*;
import java.util.Properties;

import com.jcraft.jsch.*;

public class SftpDownload {

    public SftpDownload() {
    }

    void connect(Properties prop, Session session, Channel channel, ChannelSftp channelSftp) throws Exception {
        String SFTPHOST = prop.getProperty("SFTPHOST");
        String SFTPPORT = prop.getProperty("SFTPPORT");
        String SFTPUSER = prop.getProperty("SFTPUSER");
        String SFTPPASS = prop.getProperty("SFTPPASS");

        if((SFTPHOST!=null)&&(SFTPPORT!=null)&&(SFTPUSER!=null)&&(SFTPPASS!=null)) {
            JSch jsch = new JSch();
            session = jsch.getSession(SFTPUSER, SFTPHOST, Integer.parseInt(SFTPPORT));
            session.setPassword(SFTPPASS);
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect();
            channel = session.openChannel("sftp");
            channel.connect();
            //Download file
            downloadFile(prop, session, channel, channelSftp);
        }
        else {
            String err = prop.getProperty("ERROR_PARAMETERS");
            throw new Exception(err);
        }
    }

    public void downloadFile(Properties prop, Session session, Channel channel, ChannelSftp channelSftp) throws Exception {

        String SFTPSERVERDIR = prop.getProperty("SFTPSERVERDIR");
        String POLICYID = prop.getProperty("POLICYID");
        String SFTPCLIENTDIR = prop.getProperty("SFTPCLIENTDIR");
        String SERVERFILENAME = prop.getProperty("SERVERFILENAME");
        String CLIENTFILENAME = prop.getProperty("CLIENTFILENAME");
        if((SFTPSERVERDIR!=null)&&(POLICYID!=null)&&(SFTPCLIENTDIR!=null)&&(SERVERFILENAME!=null)&&(CLIENTFILENAME!=null)) {
            channelSftp = (ChannelSftp) channel;
            channelSftp.cd(SFTPSERVERDIR + "/" + POLICYID);
            byte[] buffer = new byte[1024];
            BufferedInputStream bis = new BufferedInputStream(channelSftp.get(SERVERFILENAME));
            File newFile = new File(SFTPCLIENTDIR + "/" + CLIENTFILENAME);
            System.out.println("newFile= " + newFile);
            OutputStream os = new FileOutputStream(newFile);
            BufferedOutputStream bos = new BufferedOutputStream(os);
            int readCount;
            //System.out.println("Getting: " + theLine);
            while ((readCount = bis.read(buffer)) > 0) {
                System.out.println("Writing: ");
                System.out.println("readCount: " + readCount);
                bos.write(buffer, 0, readCount);
            }
            bis.close();
            bos.close();
        }
        else {
            //Disconnect
            disconnect(session);
            String err = prop.getProperty("ERROR_PARAMETERS");
            throw new Exception(err);
        }
        //Disconnect
        disconnect(session);
    }



    public void disconnect(Session session){
        session.disconnect();
    }
}
//- See more at: http://kodehelp.com/java-program-for-downloading-file-from-sftp-server/#sthash.bbGDFEWn.dpuf