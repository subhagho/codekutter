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
