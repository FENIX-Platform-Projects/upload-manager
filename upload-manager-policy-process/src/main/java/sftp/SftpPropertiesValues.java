package sftp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class SftpPropertiesValues {
    String result = "";
    InputStream inputStream;

    public Properties getPropValues(char type) throws IOException {
        Properties prop = new Properties();
        try {
            String propFileName = "policy_mainConfig.properties";

            switch (type){
                case 'd':
                    propFileName = "policy_datasources.properties";
                    break;
                default:
                    propFileName = "policy_mainConfig.properties";
            }

            inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }

            // get the property value and print it out
//            String user = prop.getProperty("user");
//            String company1 = prop.getProperty("company1");
//            String company2 = prop.getProperty("company2");
//            String company3 = prop.getProperty("company3");

        } catch (Exception e) {
            System.out.println("Exception: " + e);
        } finally {
            inputStream.close();
        }
        return prop;
    }
}
