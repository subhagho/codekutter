package com.codekutter.common.model.annotations;

/**
 * Enumeration for defining copy type
 * for entity properties to be used while copying.
 */
public enum ECopyType {
    /**
     * Ignore this fields while copying.
     */
    Ignore,
    /**
     * Copy a reference of this field to the target.
     */
    Reference,
    /**
     * Default Behavior as per data type.
     */
    Default
}
