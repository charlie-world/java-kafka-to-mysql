package com.charlieworld.kafkatomysql;

/**
 * Writer Charlie Lee
 * Created at 2018. 2. 27.
 */
public class MySqlRunner {

    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    String userName = null;
    String passWord = null;
    String host = null;
    int port = 3306;

    public MySqlRunner(String host, int port) {
        this.host = host;
        this.port = port;
    }

    private String getDbUrl() {
        return String.format("jdbc:mysql://%s:%d", host, port);
    }

    public String getHost() {
        return this.host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return this.port;
    }

    public void setPort(int port) {
        this.port = port;
    }


}
