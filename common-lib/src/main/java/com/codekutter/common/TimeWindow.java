/*
 *
 *  * Copyright 2014 Subhabrata Ghosh
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.codekutter.common;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class represent a time window definition. Definition includes a Granularity
 * (MILLI, SEC., MIN, etc.) and resolution (multiplier for this granularity).
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 02/08/14
 */
@Data
public class TimeWindow {
    private static final String TW_MILLISECONDS = "ms";
    private static final String TW_SECONDS = "ss";
    private static final String TW_MINUTES = "mi";
    private static final String TW_HOURS = "hh";
    private static final String TW_DAYS = "dd";

    private TimeUnit granularity;
    private int resolution;
    private long div = -1;

    /**
     * Get the time window the specified timestamp falls into.
     *
     * @param timestamp
     *            - Input timestamp.
     * @return - Time window represented as the milliseconds value.
     * @throws TimeWindowException
     */
    public long windowStart(long timestamp) throws TimeWindowException {
        return ((timestamp / div()) * div());
    }

    /**
     * Get the create and end timestamps for the window the specified timestamp
     * falls in.
     *
     * @param timestamp
     *            - Input timestamp.
     * @return - Long array[2] : array[1]=create time, array[2]=end time.
     * @throws TimeWindowException
     */
    public long[] window(long timestamp) throws TimeWindowException {
        long[] w = new long[2];

        w[0] = windowStart(timestamp);
        w[1] = w[0] + div();

        return w;
    }

    /**
     * Get the interval between the specified timestamp and the window end
     * timestamp.
     *
     * @param timestamp
     *            - Input timestamp.
     * @return - Long delta.
     * @throws TimeWindowException
     */
    public long interval(long timestamp) throws TimeWindowException {
        long[] w = window(timestamp);

        return w[1] - timestamp;
    }

    /**
     * Get the window period in milliseconds.
     *
     * @return - The window period.
     * @throws TimeWindowException
     */
    public long period() throws TimeWindowException {
        return div();
    }

    /**
     * Default to string representation of this instance.
     *
     * @return - String representation.
     */
    @Override
    public String toString() {
        return String.format("TIME WINDOW: [GRANULARITY:%s][RESOLUTION:%d]", granularity.name(),
                resolution);
    }

    private long div() throws TimeWindowException {
        if (div < 0) {
            switch (granularity) {
            case MILLISECONDS:
                div = 1;
                break;
            case SECONDS:
                div = 1000;
                break;
            case MINUTES:
                div = 1000 * 60;
                break;
            case HOURS:
                div = 1000 * 60 * 60;
                break;
            case DAYS:
                div = 1000 * 60 * 60 * 24;
                break;
            default:
                throw new TimeWindowException("Granularity not supported. [granularity = "
                        + granularity.name() + "]");
            }
            div *= resolution;
        }
        return div;
    }

    /**
     * Parse the passed string as Time window. Time Window formats:
     * [VALUE][UNIT] UNITS: - ms : milliseconds - ss : seconds - mm : minutes -
     * hh : hours - dd : days
     *
     * @param unit
     *            - Time Window string
     * @return - Parsed time window.
     * @throws TimeWindowException
     */
    public static TimeWindow parse(String unit) throws TimeWindowException {
        if (StringUtils.isNumeric(unit)) {
            TimeWindow tw = new TimeWindow();
            tw.setGranularity(TimeUnit.MILLISECONDS);
            tw.setResolution(Integer.parseInt(unit));

            return tw;
        } else {
            unit = unit.toUpperCase();
            Pattern p = Pattern.compile("(\\d+)(MS|ms|SS|ss|MI|mi|HH|hh|DD|dd{1}$)");

            Matcher m = p.matcher(unit);
            if (m.matches()) {
                if (m.groupCount() >= 2) {
                    String r = m.group(1);
                    String g = m.group(2);
                    if (!StringUtils.isEmpty(r) && !StringUtils.isEmpty(g)) {
                        TimeWindow tw = new TimeWindow();
                        tw.setResolution(Integer.parseInt(r));
                        tw.setGranularity(timeunit(g));

                        return tw;
                    }
                }
            }
        }
        throw new TimeWindowException("Cannot parse Time Window from String. [string=" + unit + "]");
    }

    /**
     * Get {@link TimeUnit} corresponding to the specified string
     *
     * @param s
     *            the string for which TimeUnit needs to be returned
     * @return the {@link TimeUnit} instance
     * @throws TimeWindowException
     *             the time window exception
     */
    private static TimeUnit timeunit(String s) throws TimeWindowException {
        if (s.compareToIgnoreCase(TW_MILLISECONDS) == 0) {
            return TimeUnit.MILLISECONDS;
        } else if (s.compareToIgnoreCase(TW_SECONDS) == 0) {
            return TimeUnit.SECONDS;
        } else if (s.compareToIgnoreCase(TW_MINUTES) == 0) {
            return TimeUnit.MINUTES;
        } else if (s.compareToIgnoreCase(TW_HOURS) == 0) {
            return TimeUnit.HOURS;
        } else if (s.compareToIgnoreCase(TW_DAYS) == 0) {
            return TimeUnit.DAYS;
        }
        throw new TimeWindowException("Invalid TimeUnit value. [string=" + s + "]");
    }
}
