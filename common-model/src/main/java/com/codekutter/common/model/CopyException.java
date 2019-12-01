package com.codekutter.common.model;

/**
 * Exception type to be used for escalating Entity Copy errors.
 */
public class CopyException extends Exception {
    private static final String __PREFIX__ = "Copy Error : %s";

    /**
     * Constructor with Error message.
     *
     * @param mesg - Error Message.
     */
    public CopyException(String mesg) {
        super(String.format(__PREFIX__, mesg));
    }

    /**
     * Constructor with Error message and root cause.
     *
     * @param mesg  - Error Message.
     * @param cause - Cause.
     */
    public CopyException(String mesg, Throwable cause) {
        super(String.format(__PREFIX__, mesg), cause);
    }

    /**
     * Constructor with root cause.
     *
     * @param cause - Cause.
     */
    public CopyException(Throwable cause) {
        super(String.format(__PREFIX__, cause.getLocalizedMessage()), cause);
    }
}
