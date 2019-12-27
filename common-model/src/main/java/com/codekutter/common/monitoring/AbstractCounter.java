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

package com.codekutter.common.monitoring;

import com.codekutter.common.TimeWindow;
import lombok.Data;

import java.util.UUID;

import static com.codekutter.common.utils.LogUtils.*;

/**
 * Abstract base class for defining counters.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 04/08/14
 */
@Data
public abstract class AbstractCounter implements Comparable<AbstractCounter> {
    /** Enum indicating the counter mode based on the log level */
    public static enum Mode {
        DEBUG, PROD
    }

    /** counter namespace */
    protected String namespace;
    /** counter name */
    protected String name;
    /** unique id */
    protected String id;
    /** type of counter based on measure (Averge or Count) */
    protected Class<? extends AbstractMeasure> type;
    /** default counter logging mode */
    protected Mode mode = Mode.DEBUG;
    /** time window def */
    protected TimeWindow windowdef;
    /** time window */
    protected long window;
    /** total counter measure since application start */
    protected AbstractMeasure total;
    /** counter measure for the defined window period */
    protected AbstractMeasure period;
    /** delta counter measure */
    protected AbstractMeasure delta;

    /**
     * Create a new instance of a counter with the specified measure type.
     *
     * @param windowdef
     *            - Time Window for recycle.
     * @param type
     *            - Measure Type (Average or Count).
     */
    protected AbstractCounter(TimeWindow windowdef, Class<? extends AbstractMeasure> type) {
        try {
            this.windowdef = windowdef;
            this.type = type;
            window = this.windowdef.windowStart(System.currentTimeMillis());
            id = UUID.randomUUID().toString();
        } catch (Throwable t) {
            debug(getClass(), t);
        }
    }

    /**
     * Get the measure type this counter supports.
     *
     * @return - Measure type.
     */
    public Class<? extends AbstractMeasure> type() {
        return type;
    }

    /**
     * Default to string implementation.
     *
     * @return - String representation.
     */
    @Override
    public String toString() {
        StringBuffer buff = new StringBuffer();
        buff.append(String.format("{[%s] %s.%s : [", type.getSimpleName().toUpperCase(), namespace,
                name));
        buff.append(String.format("[DELTA: %s]", delta.toString()));
        buff.append(String.format("[PERIOD: %s]", period.toString()));
        buff.append(String.format("[TOTAL: %s]", total.toString()));
        buff.append("]}");

        return buff.toString();
    }

    /**
     * Get the Total counter value. Total value represents the cumulative since
     * the counter started.
     *
     * @return - Measure total.
     */
    public AbstractMeasure total() {
        try {
            AbstractMeasure m = type.newInstance();

            m.add(total);
            m.add(period);
            m.add(delta);

            return m;
        } catch (Throwable t) {
            // Do nothing...
            debug(getClass(), t);
        }
        return null;
    }

    /**
     * Get the current period counter value. Period value represents the
     * cumulative for the current period. Periods are defined using the
     * configured recycle time window.
     *
     * @return - Period measure.
     */
    public AbstractMeasure period() {
        try {
            AbstractMeasure m = type.newInstance();

            m.add(period);
            m.add(delta);

            return m;
        } catch (Throwable t) {
            // Do nothing...
            debug(getClass(), t);
        }
        return null;
    }

    /**
     * Get the unread delta counter value. Deltas are incremental values
     * computed since the last read with the clear flag.
     *
     * @param clear
     *            - Clear the delta value?
     * @return - Delta measure
     */
    public AbstractMeasure delta(boolean clear) {
        try {
            AbstractMeasure d = delta;
            if (clear) {
                if (isPeriodExpired()) {
                    total.add(period);
                    period.clear();
                    period.window(windowdef.windowStart(System.currentTimeMillis()));
                }
                period.add(delta);
                delta.clear();
                delta.window(System.currentTimeMillis());
            }
            return d;

        } catch (Throwable t) {
            // Do nothing...
            debug(getClass(), t);
            return delta;
        }
    }

    /**
     * Check if the period has expired i.e. the window does not fall into the
     * window corresppnding to current time
     *
     * @return has period expired?
     */
    private boolean isPeriodExpired() {
        try {
            if (window != windowdef.windowStart(System.currentTimeMillis())) {
                window = windowdef.windowStart(System.currentTimeMillis());
                return true;
            }
        } catch (Throwable t) {
            // Do nothing...
            debug(getClass(), t);
        }
        return false;
    }

    /**
     * Merge the specified counter with the current counter by updating the
     * measures
     *
     * @param c
     *            the counter from which the measures need to be merged to this
     *            counter object
     */
    public void merge(AbstractCounter c) {
        this.delta.add(c.delta);
        this.period.add(c.period);
        this.total.add(c.total);
    }

    /**
     * Comparison function to sort counters based on namespaces.
     *
     * @param o
     *            - Target counter.
     * @return - Compare result.
     */
    @Override
    public int compareTo(AbstractCounter o) {
        return getNamespace().compareTo(o.getNamespace());
    }

    /**
     * Abstract method for creating a copy/clone of this counter instance.
     *
     * @return - Created counter copy.
     */
    public abstract AbstractCounter copy();

    /**
     * Get the key used for Hashing this counter instance.
     *
     * @return - String key for Hashing.
     */
    public String key() {
        return getKey(namespace, name);
    }

    /**
     * Utility function to get the key used for Hashing this counter instance.
     *
     * @param namespace
     *            - Counter namespace.
     * @param name
     *            - Counter name.
     * @return - String key for Hashing.
     */
    public static String getKey(String namespace, String name) {
        return namespace + ":" + name;
    }

}
