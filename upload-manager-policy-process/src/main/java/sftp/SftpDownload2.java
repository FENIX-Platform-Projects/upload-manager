package sftp;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

public class SftpDownload2 {

    public SftpDownload2() {
    }

    public static void main(String[] args) {

        // String SFTPWORKINGDIR = "/export/home/kodehelp/";
        String SFTPHOST = "EXLPRAMIS1.ext.fao.org";
        int    SFTPPORT = 22;
        String SFTPUSER = "root";
        String SFTPPASS = "j4RT$j76";
        String SFTPSERVERDIR = "/usr/local/tomcat-amis-policy/webapps/PolicyFileUpload";
        String SFTPCLIENTDIR = "/home/barbara/Documenti/Policies/FromUpload";
        String SERVERFILENAME = "01_sdmx_cog_annex_1_cdc_2009.pdf";
        String CLIENTFILENAME = "test.pdf";
        String POLICYID = "4";

        Session     session     = null;
        Channel     channel     = null;
        ChannelSftp channelSftp = null;

        try{
            JSch jsch = new JSch();
            session = jsch.getSession(SFTPUSER,SFTPHOST,SFTPPORT);
            session.setPassword(SFTPPASS);
            java.util.Properties config = new java.util.Properties();
            //If this property is set to ``no'', jsch will automatically
            // add new host keys to the user known hosts files.
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect();
            channel = session.openChannel("sftp");
            channel.connect();
            channelSftp = (ChannelSftp)channel;
            channelSftp.cd(SFTPSERVERDIR+"/"+POLICYID);
            byte[] buffer = new byte[1024];
            BufferedInputStream bis = new BufferedInputStream(channelSftp.get(SERVERFILENAME));
            File newFile = new File(SFTPCLIENTDIR+"/"+CLIENTFILENAME);
            OutputStream os = new FileOutputStream(newFile);
            BufferedOutputStream bos = new BufferedOutputStream(os);
            int readCount;
//System.out.println("Getting: " + theLine);
            while( (readCount = bis.read(buffer)) > 0) {
                System.out.println("Writing: " );
                bos.write(buffer, 0, readCount);
            }
            bis.close();
            bos.close();
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
}
//- See more at: http://kodehelp.com/java-program-for-downloading-file-from-sftp-server/#sthash.bbGDFEWn.dpuf