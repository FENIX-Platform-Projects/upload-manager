package org.fao.ess.uploader.core.init;

import javax.enterprise.context.ApplicationScoped;
import javax.servlet.ServletContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@ApplicationScoped
public class UploaderConfig extends HashMap<String,String> {

    //INIT
    public void init(File file) throws Exception {
        if (file!=null)
            init(new FileInputStream(file));
    }

    public void init (InputStream inputStream) throws Exception {
        if (inputStream!=null) {
            Properties properties = new Properties();
            properties.load(inputStream);
            init(properties);
        }
    }

    public void init (Properties properties) throws Exception {
        if (properties!=null)
            for (Object key : Collections.list(properties.propertyNames()))
                add((String)key, properties.getProperty((String) key));
    }

    public void init (ServletContext servletContext) throws Exception {
        if (servletContext!=null)
            for (String key : Collections.list(servletContext.getInitParameterNames()))
                add(key, servletContext.getInitParameter(key));
    }

    public void init (Map<String, String> parameters) throws Exception {
        if (parameters!=null)
            for (Map.Entry<String,String> entry : parameters.entrySet())
                add(entry.getKey(), entry.getValue());
    }

    //UTILS
    public void add(String key, String value) {
        if (!this.containsKey(key))
            put(key,value);
    }

}
