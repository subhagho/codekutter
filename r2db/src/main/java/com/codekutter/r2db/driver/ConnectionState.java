package com.codekutter.r2db.driver;

import com.codekutter.zconfig.common.AbstractState;

public class ConnectionState extends AbstractState<EConnectionState> {
    public ConnectionState() {
        setState(EConnectionState.Unknown);
    }

    public boolean isOpen() {
        return (getState() == EConnectionState.Open);
    }

    public boolean isClosed() {
        return (getState() == EConnectionState.Closed);
    }
}
