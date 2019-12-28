package com.codekutter.common.model;

import com.codekutter.common.IState;

/**
 * Enum defines the userState of a primary entity.
 */
public enum EEntityState implements IState<EEntityState> {
    /**
     * State is Unknown
     */
    Unknown,
    /**
     * Entity has been newly created (not persisted in DB)
     */
    New,
    /**
     * Entity has been updated.
     */
    Updated,
    /**
     * Entity is Synced with the DB.
     */
    Synced,
    /**
     * Entity record has been deleted.
     */
    Deleted,
    /**
     * Entity has been marked as In Active.
     */
    InActive,
    /**
     * Entity is in Error userState.
     */
    Error;


    /**
     * Get the userState that represents an error userState.
     *
     * @return - Error userState.
     */
    @Override
    public EEntityState getErrorState() {
        return Error;
    }
}
