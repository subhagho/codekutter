/*
 *  Copyright (2020) Subhabrata Ghosh (subho dot ghosh at outlook dot com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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
     * Entity is being synced.
     */
    Syncing,
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
