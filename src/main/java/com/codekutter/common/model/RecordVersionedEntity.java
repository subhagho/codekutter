package com.codekutter.common.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import javax.persistence.Version;

/**
 * Base class for defining entities with record version (used by hiberanate
 * for optimistic updates)
 *
 * @param <K> - Entity Key type.
 */
@Getter
@Setter
@MappedSuperclass
public abstract class RecordVersionedEntity<K> implements IEntity<K> {
    /**
     * Record Version field.
     */
    @Column(name = "record_version")
    @Version
    private long recordVersion;
}
