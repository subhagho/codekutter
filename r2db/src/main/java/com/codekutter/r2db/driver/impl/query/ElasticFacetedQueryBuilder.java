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
import com.google.common.base.Preconditions;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

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

    public ElasticFacetedQueryBuilder<T> range(@Nonnull String name,
                                               @Nonnull String field,
                                               @Nonnull Map<String, Double[]> ranges,
                                               boolean autoAppend) throws ValidationException {
        try {
            Field f = ReflectionUtils.findField(type, field);
            if (f == null) {
                throw new ReflectionException(String.format("Field not found. [type=%s][field=%s]", type.getCanonicalName(), field));
            }
            RangeAggregationBuilder rb = AggregationBuilders.range(name).field(field);
            Double[] range = null;
            boolean first = true;
            for (String key : ranges.keySet()) {
                range = ranges.get(key);
                if (first) {
                    first = false;
                    if (autoAppend) {
                        rb.addUnboundedTo("START", range[0]);
                    }
                }
                Preconditions.checkState(range.length == 2);
                rb.addRange(key, range[0], range[1]);
            }
            if (autoAppend && range != null) {
                rb.addUnboundedFrom("END", range[1]);
            }
            if (builder == null) {
                builder = rb;
            } else {
                builder.subAggregation(rb);
            }
            return this;
        } catch (Exception ex) {
            throw new ValidationException(ex);
        }
    }
    public ElasticFacetedQueryBuilder<T> range(@Nonnull String name,
                                               @Nonnull String field,
                                               @Nonnull List<Double[]> ranges,
                                               boolean autoAppend) throws ValidationException {
        try {
            Field f = ReflectionUtils.findField(type, field);
            if (f == null) {
                throw new ReflectionException(String.format("Field not found. [type=%s][field=%s]", type.getCanonicalName(), field));
            }
            RangeAggregationBuilder rb = AggregationBuilders.range(name).field(field);
            Double[] last = null;
            boolean first = true;
            for (Double[] range : ranges) {
                if (first) {
                    first = false;
                    if (autoAppend) {
                        rb.addUnboundedTo(range[0]);
                    }
                }
                Preconditions.checkState(range.length == 2);
                rb.addRange(range[0], range[1]);
                last = range;
            }
            if (autoAppend && last != null) {
                rb.addUnboundedFrom(last[1]);
            }
            if (builder == null) {
                builder = rb;
            } else {
                builder.subAggregation(rb);
            }
            return this;
        } catch (Exception ex) {
            throw new ValidationException(ex);
        }
    }

    public ElasticFacetedQueryBuilder<T> filters(@Nonnull String name, @Nonnull Map<String, String> fields) throws ValidationException {
        try {
            TermQueryBuilder[] builders = new TermQueryBuilder[fields.size()];
            int ii = 0;
            for (String field : fields.keySet()) {
                Field f = ReflectionUtils.findField(type, field);
                if (f == null) {
                    throw new ReflectionException(String.format("Field not found. [type=%s][field=%s]", type.getCanonicalName(), field));
                }
                builders[ii] = QueryBuilders.termQuery(field, fields.get(field));
                ii++;
            }
            if (builder == null) {
                builder = AggregationBuilders.filters(name, builders);
            } else {
                builder.subAggregation(AggregationBuilders.filters(name, builders));
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
