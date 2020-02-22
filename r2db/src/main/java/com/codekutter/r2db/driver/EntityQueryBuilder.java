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
import com.codekutter.common.utils.ReflectionException;
import com.codekutter.common.utils.ReflectionUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.util.List;

@SuppressWarnings("rawtypes")
public class EntityQueryBuilder<T extends IEntity> {
    private String query;
    private final Class<? extends T> type;
    private Analyzer analyzer;

    private EntityQueryBuilder(@Nonnull Class<? extends T> type) {
        this.type = type;
    }

    public EntityQueryBuilder<T> withAnalyzer(@Nonnull Analyzer analyzer) {
        this.analyzer = analyzer;
        return this;
    }


    public EntityQueryBuilder<T> group() {
        if (Strings.isNullOrEmpty(query)) query = "+(";
        else query = query + " +(";
        return this;
    }

    public EntityQueryBuilder<T> notGroup() {
        if (Strings.isNullOrEmpty(query)) query = "-(";
        else query = query + " -(";
        return this;
    }

    public EntityQueryBuilder<T> equals(@Nonnull String field, @Nonnull String value) throws ReflectionException {
        Field f = ReflectionUtils.findField(type, field);
        if (f == null) {
            throw new ReflectionException(String.format("Field not found. [type=%s][field=%s]", type.getCanonicalName(), field));
        }
        if (Strings.isNullOrEmpty(query)) query = String.format("%s:%s ", field, value);
        else query = String.format("%s +%s:%s ", query, field, value);
        return this;
    }

    public EntityQueryBuilder<T> notEquals(@Nonnull String field, @Nonnull String value) throws ReflectionException {
        Field f = ReflectionUtils.findField(type, field);
        if (f == null) {
            throw new ReflectionException(String.format("Field not found. [type=%s][field=%s]", type.getCanonicalName(), field));
        }
        if (Strings.isNullOrEmpty(query)) query = String.format("-%s:%s  ", field, value);
        else query = String.format("%s -%s:%s ", query, field, value);
        return this;
    }

    public EntityQueryBuilder<T> gt(@Nonnull String field, @Nonnull String value) throws ReflectionException {
        Field f = ReflectionUtils.findField(type, field);
        if (f == null) {
            throw new ReflectionException(String.format("Field not found. [type=%s][field=%s]", type.getCanonicalName(), field));
        }
        if (Strings.isNullOrEmpty(query)) query = String.format("%s:>%s ", field, value);
        else query = String.format("%s%s:>%s ", query, field, value);
        return this;
    }

    public EntityQueryBuilder<T> gte(@Nonnull String field, @Nonnull String value) throws ReflectionException {
        Field f = ReflectionUtils.findField(type, field);
        if (f == null) {
            throw new ReflectionException(String.format("Field not found. [type=%s][field=%s]", type.getCanonicalName(), field));
        }
        if (Strings.isNullOrEmpty(query)) query = String.format("%s:>=%s ", field, value);
        else query = String.format("%s%s:>=%s ", query, field, value);
        return this;
    }

    public EntityQueryBuilder<T> lt(@Nonnull String field, @Nonnull String value) throws ReflectionException {
        Field f = ReflectionUtils.findField(type, field);
        if (f == null) {
            throw new ReflectionException(String.format("Field not found. [type=%s][field=%s]", type.getCanonicalName(), field));
        }
        if (Strings.isNullOrEmpty(query)) query = String.format("%s:<%s ", field, value);
        else query = String.format("%s%s:<%s ", query, field, value);
        return this;
    }

    public EntityQueryBuilder<T> lte(@Nonnull String field, @Nonnull String value) throws ReflectionException {
        Field f = ReflectionUtils.findField(type, field);
        if (f == null) {
            throw new ReflectionException(String.format("Field not found. [type=%s][field=%s]", type.getCanonicalName(), field));
        }
        if (Strings.isNullOrEmpty(query)) query = String.format("%s:=<%s ", field, value);
        else query = String.format("%s%s:=<%s ", query, field, value);
        return this;
    }

    public EntityQueryBuilder<T> range(@Nonnull String field,
                                       @Nonnull String start, @Nonnull String end) throws ReflectionException {
        return range(field, start, end, true, true);
    }

    public EntityQueryBuilder<T> range(@Nonnull String field, @Nonnull String start, @Nonnull String end,
                                       boolean includeStart, boolean includeEnd) throws ReflectionException {
        Field f = ReflectionUtils.findField(type, field);
        if (f == null) {
            throw new ReflectionException(String.format("Field not found. [type=%s][field=%s]", type.getCanonicalName(), field));
        }
        StringBuffer buff = new StringBuffer(field).append(":");
        if (includeStart) buff.append("[");
        else buff.append("{");
        buff.append(start).append(" TO ").append(end);
        if (includeEnd) buff.append("]");
        else buff.append("}");

        if (Strings.isNullOrEmpty(query)) query = "+" + buff.toString();
        else query = String.format("%s +%s ", query, buff.toString());
        return this;
    }

    public EntityQueryBuilder<T> in(@Nonnull String field, @Nonnull List<String> values) throws ReflectionException {
        Field f = ReflectionUtils.findField(type, field);
        if (f == null) {
            throw new ReflectionException(String.format("Field not found. [type=%s][field=%s]", type.getCanonicalName(), field));
        }
        StringBuffer buff = new StringBuffer().append("+(");
        boolean first = true;
        for (String value : values) {
            if (first) first = false;
            else buff.append(" ");
            buff.append(field).append(":").append(value);
        }
        buff.append(")");
        if (Strings.isNullOrEmpty(query)) query = buff.toString();
        else query = String.format("%s%s ", query, buff.toString());
        return this;
    }

    public EntityQueryBuilder<T> in(@Nonnull String field, @Nonnull String[] values) throws ReflectionException {
        Field f = ReflectionUtils.findField(type, field);
        if (f == null) {
            throw new ReflectionException(String.format("Field not found. [type=%s][field=%s]", type.getCanonicalName(), field));
        }
        StringBuffer buff = new StringBuffer().append("+(");
        boolean first = true;
        for (String value : values) {
            if (first) first = false;
            else buff.append(" ");
            buff.append(field).append(":").append(value);
        }
        buff.append(")");
        if (Strings.isNullOrEmpty(query)) query = buff.toString();
        else query = String.format("%s%s ", query, buff.toString());
        return this;
    }

    public EntityQueryBuilder<T> notIn(@Nonnull String field, @Nonnull List<String> values) throws ReflectionException {
        Field f = ReflectionUtils.findField(type, field);
        if (f == null) {
            throw new ReflectionException(String.format("Field not found. [type=%s][field=%s]", type.getCanonicalName(), field));
        }
        StringBuffer buff = new StringBuffer().append("-(");
        boolean first = true;
        for (String value : values) {
            if (first) first = false;
            else buff.append(" ");
            buff.append(field).append(":").append(value);
        }
        buff.append(")");
        if (Strings.isNullOrEmpty(query)) query = buff.toString();
        else query = String.format("%s%s ", query, buff.toString());
        return this;
    }

    public EntityQueryBuilder<T> notIn(@Nonnull String field, @Nonnull String[] values) throws ReflectionException {
        Field f = ReflectionUtils.findField(type, field);
        if (f == null) {
            throw new ReflectionException(String.format("Field not found. [type=%s][field=%s]", type.getCanonicalName(), field));
        }
        StringBuffer buff = new StringBuffer().append("-(");
        boolean first = true;
        for (String value : values) {
            if (first) first = false;
            else buff.append(" ");
            buff.append(field).append(":").append(value);
        }
        buff.append(")");
        if (Strings.isNullOrEmpty(query)) query = buff.toString();
        else query = String.format("%s%s ", query, buff.toString());
        return this;
    }

    public String parse() throws ParseException {
        return query;
    }

    public EntityQueryBuilder<T> end() {
        Preconditions.checkState(!Strings.isNullOrEmpty(query));
        query = query + ") ";
        return this;
    }

    public static String string(String value) {
        return String.format("\"%s\"", value);
    }

    public static <T extends IEntity> EntityQueryBuilder<T> builder(@Nonnull Class<? extends T> type) {
        return new EntityQueryBuilder(type);
    }
}
