package com.codekutter.common.auditing;

import com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nonnull;

/**
 * Interface definition for entities that can track
 * changes. Change set is represented as a JSON patch node.
 *
 * @param <T> - Entity Type
 */
public interface IChange<T> {
    /**
     * Generate the change set compared to the entity instance
     * passed.
     *
     * @param source - Entity instance
     * @return - JSON change patch
     * @throws AuditException
     */
    JsonNode getChange(@Nonnull T source) throws AuditException;
}
