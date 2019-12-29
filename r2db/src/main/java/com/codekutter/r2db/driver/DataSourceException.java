package com.codekutter.r2db.driver;

public class DataSourceException extends Exception {
    private static final String __PREFIX = "Data Source Error : %s";

    public DataSourceException(String message) {
        super(String.format(__PREFIX, message));
    }

    public DataSourceException(String message, Throwable cause) {
        super(String.format(__PREFIX, message), cause);
    }

    public DataSourceException(Throwable cause) {
        super(String.format(__PREFIX, cause.getLocalizedMessage()), cause);
    }
}
