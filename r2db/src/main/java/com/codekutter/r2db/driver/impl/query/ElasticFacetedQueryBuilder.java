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

package com.codekutter.r2db.driver.impl.query;

import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.ValidationException;
import com.codekutter.common.utils.ReflectionException;
import com.codekutter.common.utils.ReflectionUtils;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;

@SuppressWarnings("rawtypes")
public class ElasticFacetedQueryBuilder<T extends IEntity> {
    private Class<? extends T> type;
    private AbstractAggregationBuilder builder;

    public ElasticFacetedQueryBuilder(Class<? extends T> type) {
        this.type = type;
    }

    public ElasticFacetedQueryBuilder<T> filter(@Nonnull String name, @Nonnull String field, int size) throws ValidationException {
        try {
            Field f = ReflectionUtils.findField(type, field);
            if (f == null) {
                throw new ReflectionException(String.format("Field not found. [type=%s][field=%s]", type.getCanonicalName(), field));
            }
            if (builder == null) {
                if (size <= 0)
                    builder = AggregationBuilders.terms(name).field(field);
                else
                    builder = AggregationBuilders.terms(name).field(field).size(size);
            } else {
                if (size <= 0)
                    builder.subAggregation(AggregationBuilders.terms(name).field(field));
                else
                    builder.subAggregation(AggregationBuilders.terms(name).field(field).size(size));
            }
            return this;
        } catch (Exception ex) {
            throw new ValidationException(ex);
        }
    }

    public ElasticFacetedQueryBuilder<T> dateFilter(@Nonnull String name, @Nonnull String field, @Nonnull DateHistogramInterval interval) throws ValidationException {
        try {
            Field f = ReflectionUtils.findField(type, field);
            if (f == null) {
                throw new ReflectionException(String.format("Field not found. [type=%s][field=%s]", type.getCanonicalName(), field));
            }
            if (builder == null) {
                builder = AggregationBuilders.dateHistogram(name).field(field).calendarInterval(interval);
            } else {
                builder.subAggregation(AggregationBuilders.dateHistogram(name).field(field).calendarInterval(interval));
            }
            return this;
        } catch (Exception ex) {
            throw new ValidationException(ex);
        }
    }

    public AbstractAggregationBuilder build() {
        return builder;
    }

    public static <E extends IEntity> ElasticFacetedQueryBuilder<E> builder(Class<? extends E> type) {
        return new ElasticFacetedQueryBuilder<>(type);
    }
}
