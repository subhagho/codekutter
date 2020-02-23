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

import com.codekutter.common.model.IEntity;
import com.codekutter.common.stores.BaseSearchResult;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
public class FecetedSearchResult<T extends IEntity> extends BaseSearchResult<T> {
    private Map<String, Long> facets = new HashMap<>();

    public FecetedSearchResult(@Nonnull Class<? extends IEntity> type) {
        super(type);
    }


}
