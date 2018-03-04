package com.charlieworld.kafkatomysql;

import java.sql.Statement;

public abstract class DbConnector {

    public abstract String getHost();

    public abstract void setHost(String host);

    public abstract int getPort();

    public abstract void setPort(int port);

    public abstract int executeUpdate(String sql);

    public abstract void close();

    public abstract Statement getStatement();
}
