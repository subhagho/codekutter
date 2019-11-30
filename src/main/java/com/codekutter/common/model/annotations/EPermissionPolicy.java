package com.codekutter.common.model.annotations;

public enum EPermissionPolicy {
    /**
     * Cascade Permission Check to parent(s) - Permissions specified on parents
     * will be used to filter permissions on the specified entity.
     */
    CascadeUp,
    /**
     * Entity permission will override any constraints on the parent(s) permissions.
     */
    Override
}
