package org.fao.ess.uploader.oecd.policy.bulk.utils;


import org.fao.ess.uploader.core.init.UploaderConfig;
import org.postgresql.Driver;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DataSource {

    static {
        try {
            Class.forName(Driver.class.getName());
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        }
    }

    @Inject private UploaderConfig config;
    private String url,usr,psw;

    public void init(String url, String usr, String psw) {
        this.url = url;
        this.usr = usr;
        this.psw = psw;
    }

    public Connection getConnection() throws SQLException {
        if (url==null)
            init(
                    config.get("policy.db.url"),
                    config.get("policy.db.usr"),
                    config.get("policy.db.psw")
            );
        Connection connection = DriverManager.getConnection(url, usr, psw);
        connection.setAutoCommit(false);
        return connection;
    }

}
