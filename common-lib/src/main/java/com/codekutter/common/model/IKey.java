package com.codekutter.common.model;

import java.io.Serializable;

/**
 * Define an entity Key type.
 *
 */
public interface IKey extends Serializable {
    /**
     * Get the String representation of the key.
     *
     * @return - Key String
     */
    String stringKey();

    /**
     * Compare the current key to the target.
     *
     * @param key - Key to compare to
     * @return - == 0, < -x, > +x
     */
    int compareTo(IKey key);
}
