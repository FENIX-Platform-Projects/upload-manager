package org.fao.ess.uploader.dist.init;

import org.fao.ess.uploader.core.init.UploaderConfig;

import javax.inject.Inject;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import javax.ws.rs.WebApplicationException;

@WebListener
public class Initializer implements ServletContextListener {
    @Inject UploaderConfig config;

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        try {
            config.init(this.getClass().getResourceAsStream("/mainConfig.properties"));
        } catch (Exception e) {
            throw new WebApplicationException(e);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {

    }
}
