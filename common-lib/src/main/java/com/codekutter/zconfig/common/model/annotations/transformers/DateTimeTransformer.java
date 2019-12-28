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
 * Date: 5/3/19 10:39 AM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common.model.annotations.transformers;

import com.google.common.base.Strings;
import com.codekutter.zconfig.common.GlobalConstants;
import com.codekutter.zconfig.common.model.annotations.ITransformer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * Default transformer for transforming from string to java Date.
 */
public class DateTimeTransformer implements ITransformer<Date, String> {
    protected String dateFormat = GlobalConstants.DEFAULT_DATETIME_FORMAT;

    /**
     * Transform the source value to the target type.
     *
     * @param source - Source value.
     * @return - Transformed value.
     * @throws TransformationException
     */
    @Override
    public Date transform(String source) throws TransformationException {
        if (!Strings.isNullOrEmpty(source)) {
            try {
                SimpleDateFormat format = new SimpleDateFormat(dateFormat);
                return format.parse(source);
            } catch (ParseException e) {
                throw new TransformationException(e);
            }
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
    public String reverse(Date source) throws TransformationException {
        if (source != null) {
            SimpleDateFormat format = new SimpleDateFormat(dateFormat);
            return format.format(source);
        }
        return null;
    }
}
