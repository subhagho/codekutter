package com.codekutter.common.auditing;

import org.apache.commons.lang3.SerializationException;

import javax.annotation.Nonnull;

/**
 * Interface to implement entity record serializer/de-serializer.
 *
 * @param <T> - Type of records supported
 */
public interface IAuditSerDe<T> {
    /**
     * Serialize the specified entity record.
     *
     * @param record - Entity record.
     * @param type   - Entity type being serialized.
     * @return - Serialized Byte array.
     * @throws SerializationException
     */
    @Nonnull
    byte[] serialize(@Nonnull T record, @Nonnull Class<? extends T> type) throws SerializationException;

    /**
     * Read the entity record from the byte array passed.
     *
     * @param data - Input Byte data.
     * @param type - Entity type being serialized.
     * @return - De-serialized entity record.
     * @throws SerializationException
     */
    @Nonnull
    T deserialize(@Nonnull byte[] data, @Nonnull Class<? extends T> type) throws SerializationException;
}
