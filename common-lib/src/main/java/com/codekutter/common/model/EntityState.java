package com.codekutter.common.model;

import com.codekutter.common.AbstractState;

/**
 * Class for defining states for Primary Entities.
 */
public class EntityState extends AbstractState<EEntityState> {
    /**
     * Default Constructor - Initializes userState to unknown.
     */
    public EntityState() {
        setState(EEntityState.Unknown);
    }
}
