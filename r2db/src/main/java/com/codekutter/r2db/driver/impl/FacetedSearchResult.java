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
import java.util.Set;

@Getter
@Setter
@SuppressWarnings("rawtypes")
public class FacetedSearchResult<T extends IEntity> extends BaseSearchResult<T> {
    private Map<String, FacetResult> facets = new HashMap<>();

    public FacetedSearchResult(@Nonnull Class<? extends IEntity> type) {
        super(type);
    }

    public Set<String> keys() {
        if (!facets.isEmpty()) return facets.keySet();
        return null;
    }

    public Long value(@Nonnull String facet, @Nonnull String key) {
        if (facets.containsKey(facet)) return facets.get(facet).results.get(key);
        return null;
    }

    public void clear() {
        facets.clear();
    }

    public boolean isEmpty() {
        return facets.isEmpty();
    }

    public void add(@Nonnull String facet, @Nonnull String key, @Nonnull Long value) {
        if (!facets.containsKey(facet)) {
            facets.put(facet, new FacetResult(facet));
        }
        facets.get(facet).results.put(key, value);
    }

    @Getter
    @Setter
    public static class FacetResult {
        private String name;
        private Map<String, Long> results = new HashMap<>();
        private Map<String, FacetResult> nested = null;

        public FacetResult() {
        }

        public FacetResult(@Nonnull String name) {
            this.name = name;
        }

        public void addNested(@Nonnull String name, @Nonnull String key, @Nonnull Long value) {
            if (nested == null) nested = new HashMap<>();
            if (!nested.containsKey(name)) {
                nested.put(name, new FacetResult(name));
            }
            nested.get(name).results.put(key, value);
        }
    }
}
