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

package com.codekutter.r2db.driver.impl;

import com.codekutter.common.GlobalConstants;
import com.codekutter.common.stores.impl.DataStoreAuditContext;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class FileDataStoreContext extends DataStoreAuditContext {
    private String directory;
    /**
     * Get the context as a JSON string.
     *
     * @return - JSON String
     * @throws JsonGenerationException
     */
    @Override
    public String json() throws JsonGenerationException {
        try {
            return GlobalConstants.getJsonMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new JsonGenerationException(e);
        }
    }
}
