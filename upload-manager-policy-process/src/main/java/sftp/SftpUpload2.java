package sftp;

import com.jcraft.jsch.*;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class SftpUpload2 {

    public SftpUpload2() {
    }

    /**
     * @param args
     */
    public static void main(String[] args) {

        //loadPropertiesFile();
        System.out.println("Main Start!!!");
        String SFTPHOST = "EXLPRAMIS1.ext.fao.org";
        int    SFTPPORT = 22;
        String SFTPUSER = "root";
        String SFTPPASS = "j4RT$j76";
        String SFTPSERVERDIR = "/usr/local/tomcat-amis-policy/webapps/PolicyFileUpload";
        String SFTPCLIENTDIR = "/home/barbara/Documenti/Policies/Metadato";
        String FILENAME = "01_sdmx_cog_annex_1_cdc_2009.pdf";
        String POLICYID = "4";

        Session     session     = null;
        Channel     channel     = null;
        ChannelSftp channelSftp = null;

        try{
            JSch jsch = new JSch();
            session = jsch.getSession(SFTPUSER,SFTPHOST,SFTPPORT);
            session.setPassword(SFTPPASS);
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect();


            channel = session.openChannel("sftp");
            channel.connect();
            channelSftp = (ChannelSftp)channel;

            String newUploadDirectory = SFTPSERVERDIR+"/"+POLICYID;
            channelSftp.cd(newUploadDirectory);

            //Qui collegare il file messo in input
            System.out.println(SFTPCLIENTDIR+"/"+FILENAME);
            File f = new File(SFTPCLIENTDIR+"/"+FILENAME);
            System.out.println("Before sending file....");
            channelSftp.put(new FileInputStream(f), f.getName());
            channelSftp.disconnect();
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }

//    void loadPropertiesFile(){
//        try{
//            Properties prop = new Properties();
//            String propFileName = "policy_mainConfig.properties";
//            InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
//
//            if(inputStream != null){
//                prop.load(inputStream);
//            }
//            else{
//                throw new FileNotFoundException("Property file '"+propFileName+"' not found in the classpath");
//            }
//        }
//        catch(IOException ioEx){
//
//        }
//    }

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

//    public void disconnect(){
//        session.disconnect();
//    }

}
//SUBSCRIBE/FOLLOW US
//        - See more at: http://kodehelp.com/java-program-for-uploading-file-to-sftp-server/#sthash.pLYM6gKp.dpuf
//http://www.programcreek.com/java-api-examples/index.php?api=com.jcraft.jsch.ChannelSftp

//https://docs.oracle.com/javaee/6/tutorial/doc/giplj.html#gipln

