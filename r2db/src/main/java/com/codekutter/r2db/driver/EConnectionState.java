package com.codekutter.r2db.driver;


public enum EConnectionState implements IState<EConnectionState> {
    /**
     * Connection state is Unknown
     */
    Unknown,
    /**
     * Connection has been initialized.
     */
    Initialized,
    /**
     * Connection is open and available
     */
    Open,
    /**
     * Connection has been closed.
     */
    Closed,
    /**
     * Connection in error state.
     */
    Error;

    @Override
    public EConnectionState getErrorState() {
        return Error;
    }
}
