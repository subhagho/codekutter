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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;

@SuppressWarnings("rawtypes")
public class LuceneQueryBuilder<T extends IEntity> {
    private Class<? extends T> type;
    private Query query;

    public LuceneQueryBuilder(Class<? extends T> type) {
        this.type = type;
    }

    private void addQuery(Query query, boolean not) {
        if (this.query == null) {
            if (not) {
                BooleanQuery bq = new BooleanQuery.Builder().add(query, BooleanClause.Occur.MUST_NOT).build();
                this.query = bq;
            } else
                this.query = query;
        } else {
            BooleanClause.Occur o = BooleanClause.Occur.MUST;
            if (not) o = BooleanClause.Occur.MUST_NOT;
            this.query = new BooleanQuery.Builder().add(this.query, BooleanClause.Occur.MUST).add(query, o).build();
        }
    }

    private LuceneQueryBuilder<T> notMatches(@Nonnull String field, @Nonnull String regex) throws ValidationException {
        return matches(field, regex, true);
    }

    private LuceneQueryBuilder<T> matches(@Nonnull String field, @Nonnull String regex) throws ValidationException {
        return matches(field, regex, false);
    }

    private LuceneQueryBuilder<T> matches(@Nonnull String field, @Nonnull String regex, boolean not) throws ValidationException {
        try {
            Field f = ReflectionUtils.findField(type, field);
            if (f == null) {
                throw new ReflectionException(String.format("Field not found. [type=%s][field=%s]", type.getCanonicalName(), field));
            }
            RegexpQuery qb = new RegexpQuery(new Term(field, regex));
            addQuery(qb, not);
            return this;
        } catch (Exception ex) {
            throw new ValidationException(ex);
        }
    }

    public LuceneQueryBuilder<T> notInRange(@Nonnull String field, @Nonnull String start, @Nonnull String end) throws ValidationException {
        return range(field, start, end, true, true, true);
    }

    public LuceneQueryBuilder<T> notInRange(@Nonnull String field, @Nonnull String start, @Nonnull String end,
                                            boolean includeStart, boolean includeEnd) throws ValidationException {
        return range(field, start, end, includeStart, includeEnd, true);
    }

    public LuceneQueryBuilder<T> range(@Nonnull String field, @Nonnull String start, @Nonnull String end) throws ValidationException {
        return range(field, start, end, true, true, false);
    }

    public LuceneQueryBuilder<T> range(@Nonnull String field, @Nonnull String start, @Nonnull String end,
                                       boolean includeStart, boolean includeEnd) throws ValidationException {
        return range(field, start, end, includeStart, includeEnd, false);
    }

    public LuceneQueryBuilder<T> range(@Nonnull String field, @Nonnull String start, @Nonnull String end,
                                       boolean includeStart, boolean includeEnd, boolean not) throws ValidationException {
        try {
            Field f = ReflectionUtils.findField(type, field);
            if (f == null) {
                throw new ReflectionException(String.format("Field not found. [type=%s][field=%s]", type.getCanonicalName(), field));
            }
            TermRangeQuery qb = new TermRangeQuery(field, new BytesRef(start), new BytesRef(end), includeStart, includeEnd);
            addQuery(qb, not);
            return this;
        } catch (Exception ex) {
            throw new ValidationException(ex);
        }
    }

    public LuceneQueryBuilder<T> should(@Nonnull String field, @Nonnull String... values) throws ValidationException {
        return should(field, false, values);
    }

    public LuceneQueryBuilder<T> shouldNot(@Nonnull String field, @Nonnull String... values) throws ValidationException {
        return should(field, true, values);
    }

    private LuceneQueryBuilder<T> should(@Nonnull String field, boolean not, @Nonnull String... values) throws ValidationException {
        Preconditions.checkArgument(values.length > 0);
        try {
            Field f = ReflectionUtils.findField(type, field);
            if (f == null) {
                throw new ReflectionException(String.format("Field not found. [type=%s][field=%s]", type.getCanonicalName(), field));
            }
            Query qb = null;
            if (values.length == 1) {
                qb = new TermQuery(new Term(field, values[0]));
            } else {
                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                for (String value : values) {
                    TermQuery tq = new TermQuery(new Term(field, value));
                    builder.add(tq, BooleanClause.Occur.SHOULD);
                }
                qb = builder.build();
            }
            addQuery(qb, not);
            return this;
        } catch (Exception ex) {
            throw new ValidationException(ex);
        }
    }

    public LuceneQueryBuilder<T> notTerm(@Nonnull String field, @Nonnull String... values) throws ValidationException {
        return term(field, true, values);
    }

    public LuceneQueryBuilder<T> term(@Nonnull String field, @Nonnull String... values) throws ValidationException {
        return term(field, false, values);
    }

    private LuceneQueryBuilder<T> term(@Nonnull String field, boolean not, @Nonnull String... values) throws ValidationException {
        Preconditions.checkArgument(values.length > 0);
        try {
            Field f = ReflectionUtils.findField(type, field);
            if (f == null) {
                throw new ReflectionException(String.format("Field not found. [type=%s][field=%s]", type.getCanonicalName(), field));
            }
            Query qb = null;
            if (values.length == 1) {
                qb = new TermQuery(new Term(field, values[0]));
            } else {
                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                for (String value : values) {
                    TermQuery tq = new TermQuery(new Term(field, value));
                    builder.add(tq, BooleanClause.Occur.MUST);
                }
                qb = builder.build();
            }
            addQuery(qb, not);
            return this;
        } catch (Exception ex) {
            throw new ValidationException(ex);
        }
    }

    public Query build() {
        return query;
    }

    public static <E extends IEntity> LuceneQueryBuilder<E> builder(Class<? extends E> type) {
        return new LuceneQueryBuilder<>(type);
    }
}
