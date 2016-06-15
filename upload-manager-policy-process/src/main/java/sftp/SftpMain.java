package sftp;

import com.jcraft.jsch.*;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class SftpMain {

    public static void main(String[] args) throws IOException {
        SftpPropertiesValues properties = new SftpPropertiesValues();
        //Main Properties file
        Properties prop = properties.getPropValues('m');

        SftpUpload sftpUpload = new SftpUpload();
        //SftpDownload sftpDownload = new SftpDownload();
        //SftpRemove sftpRemove = new SftpRemove();
        Session session     = null;
        Channel channel     = null;
        ChannelSftp channelSftp = null;

        String SFTPCLIENTDIR = prop.getProperty("SFTPCLIENTDIR");
        String FILENAME = prop.getProperty("SERVERFILENAME");
        //Connection
        try {
            //From client:1-File obj;2-File name;3-Policy Id
            System.out.println("Before connect...");
            File f = new File(SFTPCLIENTDIR + "/" + FILENAME);
            String policyId = "10";
            sftpUpload.connect(prop, session, channel, channelSftp, f, "test", policyId);
            //sftpDownload.connect(prop, session, channel, channelSftp);
            //sftpRemove.connect(prop, session, channel, channelSftp, "01_sdmx_cog_annex_1_cdc_2009.pdf", policyId);

        } catch (JSchException e) {
            //Return that the action has not been performed
            e.printStackTrace();
        }
        catch (SftpException e) {
            //Return that the action has not been performed
            e.printStackTrace();
        }
        catch (Exception e) {
            //Return that the action has not been performed
            e.printStackTrace();
        }
    }
}
