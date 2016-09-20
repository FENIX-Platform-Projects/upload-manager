package org.fao.ess.uploader.gift.bulk.dto;

public class HostProperties {
    private String protocol;
    private String host;
    private Integer port;
    private String user;
    private String password;
    private String path;


    public HostProperties() {}

    public HostProperties(String protocol, String host, Integer port) {
        this.protocol = protocol;
        this.host = host;
        this.port = port;
    }

    public HostProperties(String protocol, String host, Integer port, String user, String password, String path) {
        this.protocol = protocol;
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.path = path!=null && path.endsWith("/") ? path.substring(0, path.length()-1) : path;
    }


    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
