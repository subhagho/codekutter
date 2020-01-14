package com.codekutter.common.model;

/**
 * Interface represents a object instance with a key.
 *
 * @param <K> - Key class.
 */
public interface IKeyed<K extends IKey> {
    /**
     * Get the object instance Key.
     *
     * @return - Key
     */
    K getKey();
}
