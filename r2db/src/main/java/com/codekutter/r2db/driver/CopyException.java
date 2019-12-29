package com.codekutter.r2db.driver;

public class CopyException extends Exception {
    private static final String __PREFIX = "Copy Error : %s";

    public CopyException(String message) {
        super(String.format(__PREFIX, message));
    }

    public CopyException(String message, Throwable cause) {
        super(String.format(__PREFIX, message), cause);
    }

    public CopyException(Throwable cause) {
        super(String.format(__PREFIX, cause.getLocalizedMessage()), cause);
    }
}
