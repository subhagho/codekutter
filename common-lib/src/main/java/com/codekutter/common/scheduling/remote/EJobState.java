package com.codekutter.common.scheduling.remote;

import com.codekutter.common.IState;

public enum EJobState implements IState<EJobState> {
    Unknown, Pending, Running, Error, Stopped, Finished;

    /**
     * Get the state that represents an error state.
     *
     * @return - Error state.
     */
    @Override
    public EJobState getErrorState() {
        return Error;
    }
}
