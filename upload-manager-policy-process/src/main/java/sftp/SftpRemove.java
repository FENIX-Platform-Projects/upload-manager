package sftp;

import com.jcraft.jsch.*;

import java.util.Properties;

public class SftpRemove {

    public SftpRemove(){

    }

    void connect(Properties prop, Session session, Channel channel, ChannelSftp channelSftp, String filename, String policyId) throws Exception {
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
            removeFile(prop, session, channel, channelSftp, filename, policyId);
        }
        else {
            String err = prop.getProperty("ERROR_PARAMETERS");
            throw new Exception(err);
        }
    }

    public void removeFile(Properties prop, Session session, Channel channel, ChannelSftp channelSftp, String filename, String policyId) throws Exception {

        String SFTPSERVERDIR = prop.getProperty("SFTPSERVERDIR");

        if((filename!=null)&&(policyId!=null)){
            channel = session.openChannel("sftp");
            channel.connect();
            channelSftp = (ChannelSftp) channel;

            String deleteFilePath = SFTPSERVERDIR + "/" + policyId + "/" + filename;
            System.out.println("deleteFilePath = "+deleteFilePath);
            channelSftp.rm(deleteFilePath);
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

    public void disconnect(Session session){
        session.disconnect();
    }
}
