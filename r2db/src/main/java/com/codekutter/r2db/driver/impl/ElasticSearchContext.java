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

import com.codekutter.common.Context;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.search.sort.SortBuilder;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

public class ElasticSearchContext extends Context {
    public static final String CONTEXT_ES_SCROLL_ID = "context.es.scroll.id";
    public static final String CONTEXT_ES_SCROLL= "context.es.scroll";
    public static final String CONTEXT_ES_FUZZINESS = "context.es.fuzziness";
    public static final String CONTEXT_ES_SORT = "context.es.sort";

    public ElasticSearchContext doScroll(boolean scroll) {
        setParam(CONTEXT_ES_SCROLL, scroll);
        return this;
    }

    public boolean doScroll() {
        return getBoolParam(CONTEXT_ES_SCROLL);
    }

    public ElasticSearchContext scrollId(@Nonnull String scrollId) {
        setParam(CONTEXT_ES_SCROLL_ID, scrollId);
        return this;
    }

    public String scrollId() {
        return getStringParam(CONTEXT_ES_SCROLL_ID);
    }

    public ElasticSearchContext fuzziness(Fuzziness fuzziness) {
        setParam(CONTEXT_ES_FUZZINESS, fuzziness);
        return this;
    }

    public Fuzziness fuzziness() {
        return (Fuzziness) getParam(CONTEXT_ES_FUZZINESS);
    }

    public ElasticSearchContext sort(@Nonnull SortBuilder<?> builder) {
        List<SortBuilder<?>> builders = (List<SortBuilder<?>>)getParam(CONTEXT_ES_SORT);
        if (builders == null) {
            builders = new ArrayList<>();
            setParam(CONTEXT_ES_SORT, builders);
        }
        builders.add(builder);
        return this;
    }

    public List<SortBuilder<?>> sort() {
        return (List<SortBuilder<?>>)getParam(CONTEXT_ES_SORT);
    }
}
