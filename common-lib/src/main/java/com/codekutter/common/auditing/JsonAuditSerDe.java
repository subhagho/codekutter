package com.codekutter.common.auditing;

import com.codekutter.common.GlobalConstants;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.lang3.SerializationException;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;

public class JsonAuditSerDe implements IAuditSerDe<Object> {

    /**
     * Serialize the specified entity record.
     *
     * @param record - Entity record.
     * @param type   - Entity type being serialized.
     * @return - Serialized Byte array.
     * @throws SerializationException
     */
    @Nonnull
    @Override
    public byte[] serialize(@Nonnull Object record, @Nonnull Class<?> type) throws SerializationException {
        try {
            ObjectMapper mapper = GlobalConstants.getJsonMapper();
            String json = mapper.writeValueAsString(record);
            if (Strings.isNullOrEmpty(json)) {
                throw new SerializationException(String.format("Error serializing record. [type=%s]",
                        type.getCanonicalName()));
            }
            return json.getBytes(StandardCharsets.UTF_8);
        } catch (Exception ex) {
            throw new SerializationException(ex);
        }
    }

    /**
     * Read the entity record from the byte array passed.
     *
     * @param data - Input Byte data.
     * @param type - Entity type being serialized.
     * @return - De-serialized entity record.
     * @throws SerializationException
     */
    @Nonnull
    @Override
    public Object deserialize(@Nonnull byte[] data, @Nonnull Class<?> type) throws SerializationException {
        try {
            Preconditions.checkArgument(data.length > 0);
            ObjectMapper mapper = GlobalConstants.getJsonMapper();
            Object value = mapper.readValue(data, type);
            if (value == null) {
                throw new SerializationException(String.format("Error de-serializing record. [type=%s]",
                        type.getCanonicalName()));
            }
            return value;
        } catch (Exception ex) {
            throw new SerializationException(ex);
        }
    }
}
