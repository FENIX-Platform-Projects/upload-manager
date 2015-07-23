package org.fao.ess.uploader.rlm.processor;

import org.fao.ess.uploader.core.init.UploaderConfig;
import org.postgresql.Driver;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@ApplicationScoped
public class RLMDataConnector {
    static {
        try {
            Class.forName(Driver.class.getName());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private String url,usr,psw;

    @Inject UploaderConfig config;


    public Connection getConnection() throws SQLException {
        if (url==null) {
            url = config.get("rlm.data.url");
            usr = config.get("rlm.data.usr");
            psw = config.get("rlm.data.psw");
        }

        return DriverManager.getConnection(url,usr,psw);
    }
}
