package com.codekutter.common.model;

/**
 * Interface represents a object instance with a key.
 *
 * @param <K> - Key type.
 */
public interface IKeyed<K> {
    /**
     * Get the object instance Key.
     *
     * @return - Key
     */
    K getKey();

    /**
     * Get a String representation of the key.
     *
     * @return - String Key
     */
    String getStringKey();
}
