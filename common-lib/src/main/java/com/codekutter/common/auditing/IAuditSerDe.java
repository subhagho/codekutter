/*
 *  Copyright (2020) Subhabrata Ghosh (subho dot ghosh at outlook dot com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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
