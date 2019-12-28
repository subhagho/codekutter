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
 * Date: 3/1/19 4:17 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.common.utils;

import com.codekutter.common.GlobalConstants;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Utility class to parse/write datetime.
 */
public class DateTimeUtils {
    /**
     * Parse the Date/Time string using the specified format string.
     *
     * @param datetime - Date/Time string.
     * @param format   - Format string.
     * @return - Parsed DateTime.
     */
    public static DateTime parse(String datetime, String format) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(datetime));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(format));

        DateTimeFormatter formatter = DateTimeFormat.forPattern(format);
        return formatter.parseDateTime(datetime);
    }

    /**
     * Parse the Date string using the default Date Format.
     *
     * @param date - Date String
     * @return - Parsed DateTime.
     */
    public static DateTime parseDate(String date) {
        return parse(date, GlobalConstants.DEFAULT_JODA_DATE_FORMAT);
    }

    /**
     * Parse the Date/Time string using the default Date/Time Format.
     *
     * @param datetime - Date/Time string.
     * @return - Parsed DateTime.
     */
    public static DateTime parse(String datetime) {
        return parse(datetime, GlobalConstants.DEFAULT_JODA_DATETIME_FORMAT);
    }

    /**
     * Get the Date string for the specified DateTime. Uses the default Date Format.
     *
     * @param dateTime - DateTime to stringify.
     * @return - String Date.
     */
    public static String toDateString(DateTime dateTime) {
        Preconditions.checkArgument(dateTime != null);
        return dateTime.toString(GlobalConstants.DEFAULT_JODA_DATE_FORMAT);
    }

    /**
     * Get the Date/Time string for the specified DateTime. Uses the default Date/Time Format.
     *
     * @param dateTime - DateTime to stringify.
     * @return - String Date/Time.
     */
    public static String toString(DateTime dateTime) {
        Preconditions.checkArgument(dateTime != null);
        return dateTime.toString(GlobalConstants.DEFAULT_JODA_DATETIME_FORMAT);
    }

    /**
     * Get the Date/Time string for the specified DateTime. Uses the default Date/Time Format.
     *
     * @param timestamp - Timestamp to stringify.
     * @return - String Date/Time.
     */
    public static String toString(long timestamp) {
        DateTime dateTime = new DateTime(timestamp);
        return dateTime.toString(GlobalConstants.DEFAULT_JODA_DATETIME_FORMAT);
    }
}
