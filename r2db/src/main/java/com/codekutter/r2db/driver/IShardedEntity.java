package com.codekutter.r2db.driver;

import com.codekutter.common.model.IEntity;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;

import java.io.Serializable;

/**
 * Interface to be implemented for defining entities that can be sharded.
 *
 * @param <K> - Entity Key type
 * @param <S> - Shard Key type
 */
@ConfigPath(path = "entity")
public interface IShardedEntity<K, S extends Serializable> extends IEntity<K> {
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
