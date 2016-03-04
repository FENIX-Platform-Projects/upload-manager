package sftp;

import org.postgresql.Driver;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class POLICYDataConnector {
    static {
        try {
            Class.forName(Driver.class.getName());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private String url,usr,psw;

    public Connection getConnection() throws SQLException {
        if (url==null) {

            SftpPropertiesValues properties = new SftpPropertiesValues();
            //Main Properties file
            try {
                Properties prop = properties.getPropValues('d');
                url = prop.getProperty("POLICY.url");
                usr = prop.getProperty("POLICY.username");
                psw = prop.getProperty("POLICY.password");
                return DriverManager.getConnection(url,usr,psw);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return null;
    }
}
