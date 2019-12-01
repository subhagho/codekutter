/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Copyright (c) $year
 * Date: 5/3/19 9:14 AM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.common.config.annotations.transformers;

import com.codekutter.common.GlobalConstants;
import com.codekutter.common.config.annotations.ITransformer;
import com.google.common.base.Strings;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


/**
 * Default transformer for transforming from string to JodaTime.
 */
public class JodaTimeTransformer implements ITransformer<DateTime, String> {
    protected String dateFormat = GlobalConstants.DEFAULT_JODA_DATETIME_FORMAT;

    /**
     * Transform the source value to the target type.
     *
     * @param source - Source value.
     * @return - Transformed value.
     * @throws TransformationException
     */
    @Override
    public DateTime transform(String source) throws TransformationException {
        if (!Strings.isNullOrEmpty(source)) {
            DateTimeFormatter formatter = DateTimeFormat.forPattern(dateFormat);
            return formatter.parseDateTime(source);
        }
        return null;
    }

    /**
     * Transform the target value to the source type.
     *
     * @param source - Source value.
     * @return - Transformed value.
     * @throws TransformationException
     */
    @Override
    public String reverse(DateTime source) throws TransformationException {
        if (source != null) {
            DateTimeFormatter formatter = DateTimeFormat.forPattern(dateFormat);
            return source.toString(formatter);
        }
        return null;
    }
}
