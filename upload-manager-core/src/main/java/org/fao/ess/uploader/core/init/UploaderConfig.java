package org.fao.ess.uploader.core.init;

import javax.enterprise.context.ApplicationScoped;
import javax.servlet.ServletContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

@ApplicationScoped
public class UploaderConfig implements Map<String, String> {

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



    //MAP interface support
    private HashMap<String,String> properties = new HashMap<>();

    @Override
    public int size() {
        return properties.size();
    }

    @Override
    public boolean isEmpty() {
        return properties.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return properties.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return properties.containsValue(value);
    }

    @Override
    public String get(Object key) {
        return properties.get(key);
    }

    @Override
    public String put(String key, String value) {
        return properties.put(key, value);
    }

    @Override
    public String remove(Object key) {
        return properties.remove(key);
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        properties.putAll(m);
    }

    @Override
    public void clear() {
        properties.clear();
    }

    @Override
    public Set<String> keySet() {
        return properties.keySet();
    }

    @Override
    public Collection<String> values() {
        return properties.values();
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        return properties.entrySet();
    }
}
