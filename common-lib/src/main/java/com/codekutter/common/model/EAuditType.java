package com.codekutter.common.model;

/**
 * Enum type to log audit records.
 */
public enum EAuditType {
    /**
     * Entity records was created.
     */
    Create,
    /**
     * Entity records was updated
     */
    Update,
    /**
     * Entity record was deleted.
     */
    Delete,
    /**
     * Entity record was read.
     */
    Read
}
