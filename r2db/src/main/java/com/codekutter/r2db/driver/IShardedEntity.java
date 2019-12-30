package com.codekutter.r2db.driver;

import com.codekutter.common.model.IEntity;

/**
 * Interface to be implemented for defining entities that can be sharded.
 *
 * @param <K> - Entity Key type
 * @param <S> - Shard Key type
 */
public interface IShardedEntity<K, S> extends IEntity<K> {
    /**
     * Get the shard key for this entity instance.
     *
     * @return - Shard Key
     */
    S getShardKey();
}
