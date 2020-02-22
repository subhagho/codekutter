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

package com.codekutter.r2db.driver;

import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.ValidationException;
import com.codekutter.common.utils.ReflectionException;
import com.codekutter.common.utils.ReflectionUtils;
import com.google.common.base.Preconditions;
import org.elasticsearch.index.query.*;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;

@SuppressWarnings("rawtypes")
public class ElasticQueryBuilder<T extends IEntity> {
    private Class<? extends T> type;
    private QueryBuilder queryBuilder = null;

    public ElasticQueryBuilder(Class<? extends T> type) {
        this.type = type;
    }

    private void addQuery(QueryBuilder query, boolean not) {
        if (queryBuilder == null) {
            if (not) {
                BoolQueryBuilder bq = QueryBuilders.boolQuery();
                bq.mustNot(query);
                queryBuilder = bq;
            } else
                queryBuilder = query;
        } else {
            if (queryBuilder instanceof BoolQueryBuilder) {
                if (not) ((BoolQueryBuilder) queryBuilder).mustNot(query);
                else ((BoolQueryBuilder) queryBuilder).must(query);
            } else {
                BoolQueryBuilder bq = QueryBuilders.boolQuery();
                bq.must(queryBuilder);
                if (not) bq.mustNot(query);
                else bq.must(query);
            }
        }
    }
    private ElasticQueryBuilder<T> notMatches(@Nonnull String field, @Nonnull String regex) throws ValidationException {
        return matches(field, regex, true);
    }

    private ElasticQueryBuilder<T> matches(@Nonnull String field, @Nonnull String regex) throws ValidationException {
        return matches(field, regex, false);
    }

    private ElasticQueryBuilder<T> matches(@Nonnull String field, @Nonnull String regex, boolean not) throws ValidationException {
        try {
            Field f = ReflectionUtils.findField(type, field);
            if (f == null) {
                throw new ReflectionException(String.format("Field not found. [type=%s][field=%s]", type.getCanonicalName(), field));
            }
            RegexpQueryBuilder qb = QueryBuilders.regexpQuery(field, regex);
            addQuery(qb, not);
            return this;
        } catch (Exception ex) {
            throw new ValidationException(ex);
        }
    }

    public ElasticQueryBuilder<T> notInRange(@Nonnull String field, @Nonnull String start, @Nonnull String end) throws ValidationException {
        return range(field, start, end, true, true, true);
    }

    public ElasticQueryBuilder<T> notInRange(@Nonnull String field, @Nonnull String start, @Nonnull String end,
                                             boolean includeStart, boolean includeEnd) throws ValidationException {
        return range(field, start, end, includeStart, includeEnd, true);
    }

    public ElasticQueryBuilder<T> range(@Nonnull String field, @Nonnull String start, @Nonnull String end) throws ValidationException {
        return range(field, start, end, true, true, false);
    }

    public ElasticQueryBuilder<T> range(@Nonnull String field, @Nonnull String start, @Nonnull String end,
                                        boolean includeStart, boolean includeEnd) throws ValidationException {
        return range(field, start, end, includeStart, includeEnd, false);
    }

    public ElasticQueryBuilder<T> range(@Nonnull String field, @Nonnull String start, @Nonnull String end,
                                        boolean includeStart, boolean includeEnd, boolean not) throws ValidationException {
        try {
            Field f = ReflectionUtils.findField(type, field);
            if (f == null) {
                throw new ReflectionException(String.format("Field not found. [type=%s][field=%s]", type.getCanonicalName(), field));
            }
            RangeQueryBuilder qb = QueryBuilders.rangeQuery(field);
            qb.from(start, includeStart).to(end, includeEnd);
            addQuery(qb, not);
            return this;
        } catch (Exception ex) {
            throw new ValidationException(ex);
        }
    }

    public ElasticQueryBuilder<T> should(@Nonnull String field, @Nonnull String... values) throws ValidationException {
        return should(field, false, values);
    }

    public ElasticQueryBuilder<T> shouldNot(@Nonnull String field, @Nonnull String... values) throws ValidationException {
        return should(field, true, values);
    }

    private ElasticQueryBuilder<T> should(@Nonnull String field, boolean not, @Nonnull String... values) throws ValidationException {
        Preconditions.checkArgument(values.length > 0);
        try {
            Field f = ReflectionUtils.findField(type, field);
            if (f == null) {
                throw new ReflectionException(String.format("Field not found. [type=%s][field=%s]", type.getCanonicalName(), field));
            }
            QueryBuilder qb = null;
            if (values.length == 1) {
                qb = QueryBuilders.termQuery(field, values[0]);
            } else {
                qb = QueryBuilders.boolQuery();
                for (String value : values) {
                    TermQueryBuilder tq = QueryBuilders.termQuery(field, value);
                    ((BoolQueryBuilder) qb).should(tq);
                }
            }
            addQuery(qb, not);
            return this;
        } catch (Exception ex) {
            throw new ValidationException(ex);
        }
    }

    public ElasticQueryBuilder<T> notTerm(@Nonnull String field, @Nonnull String... values) throws ValidationException {
        return term(field, true, values);
    }

    public ElasticQueryBuilder<T> term(@Nonnull String field, @Nonnull String... values) throws ValidationException {
        return term(field, false, values);
    }

    private ElasticQueryBuilder<T> term(@Nonnull String field, boolean not, @Nonnull String... values) throws ValidationException {
        Preconditions.checkArgument(values.length > 0);
        try {
            Field f = ReflectionUtils.findField(type, field);
            if (f == null) {
                throw new ReflectionException(String.format("Field not found. [type=%s][field=%s]", type.getCanonicalName(), field));
            }
            QueryBuilder qb = null;
            if (values.length == 1) {
                qb = QueryBuilders.termQuery(field, values[0]);
            } else {
                qb = QueryBuilders.boolQuery();
                for (String value : values) {
                    TermQueryBuilder tq = QueryBuilders.termQuery(field, value);
                    ((BoolQueryBuilder) qb).must(tq);
                }
            }
            addQuery(qb, not);
            return this;
        } catch (Exception ex) {
            throw new ValidationException(ex);
        }
    }

    public QueryBuilder build() {
        return queryBuilder;
    }

    public static <E extends IEntity> ElasticQueryBuilder<E> builder(Class<? extends E> type) {
        return new ElasticQueryBuilder<>(type);
    }
}
