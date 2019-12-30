package com.codekutter.r2db.driver;

public class DataStoreException extends Exception {
    private static final String __PREFIX = "Data Source Error : %s";

    public DataStoreException(String message) {
        super(String.format(__PREFIX, message));
    }

    public DataStoreException(String message, Throwable cause) {
        super(String.format(__PREFIX, message), cause);
    }

    public DataStoreException(Throwable cause) {
        super(String.format(__PREFIX, cause.getLocalizedMessage()), cause);
    }
}
