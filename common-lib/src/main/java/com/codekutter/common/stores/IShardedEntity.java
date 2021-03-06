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

package com.codekutter.common.stores;

import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.IKey;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;

import java.io.Serializable;

/**
 * Interface to be implemented for defining entities that can be sharded.
 *
 * @param <S> - Shard Key type
 */
@ConfigPath(path = "entity")
public interface IShardedEntity<K extends IKey, S extends Serializable> extends IEntity<K> {
    /**
     * Get the shard key for this entity instance.
     *
     * @return - Shard Key
     */
    S getShardKey();

    /**
     * Get the shard ID for this entity.
     *
     * @return - Shard ID
     */
    int getShardId();
}
