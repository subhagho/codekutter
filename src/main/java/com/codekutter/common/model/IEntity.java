package com.codekutter.common.model;

import com.codekutter.zconfig.common.Context;

/**
 * Interface for defining entities.
 *
 * @param <K> - Entity Unique Key type.
 */
public interface IEntity<K> extends IValidate {
    /**
     * Get the unique Key for this entity.
     *
     * @return - Entity Key.
     */
    K getKey();

    /**
     * Compare the entity key with the key specified.
     *
     * @param key - Target Key.
     * @return - Comparision.
     */
    int compare(K key);

    /**
     * Copy the changes from the specified source entity
     * to this instance.
     *
     * All properties other than the Key will be copied.
     * Copy Type:
     *  Primitive - Copy
     *  String - Copy
     *  Enum - Copy
     *  Nested Entity - Copy Recursive
     *  Other Objects - Copy Reference.
     *
     * @param source - Source instance to Copy from.
     * @param context - Execution context.
     * @return - Copied Entity instance.
     * @exception CopyException
     */
    IEntity<K> copyChanges(IEntity<K> source, Context context) throws CopyException;

    /**
     * Clone this instance of Entity.
     *
     *
     * @param context - Clone Context.
     * @return - Cloned Instance.
     * @throws CopyException
     */
    IEntity<K> clone(Context context) throws CopyException;
}
