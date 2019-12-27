/*
 *  Copyright (2019) Subhabrata Ghosh (subho dot ghosh at outlook dot com)
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

package com.codekutter.common.monitoring;

import org.joda.time.DateTime;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Average implementation of {@link AbstractMeasure}. These are usually time
 * counters that are averaged over a period of time. The average value is
 * derived form the count and the sum value
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 05/08/14
 */
public class Average extends AbstractMeasure {
    /** sum value of the measure */
    private AtomicLong value = new AtomicLong(0);
    /** count of the measure */
    private AtomicLong count = new AtomicLong(0);

    /**
     * Return the sum value of the measure
     *
     * @return the value
     */
    public long value() {
        return value.get();
    }

    /**
     * Return the count of the measure
     *
     * @return the count
     */
    public long count() {
        return count.get();
    }

    /**
     * Compute and return the average from the value and count
     *
     * @return the average of this measure
     */
    public double average() {
        if (count.get() > 0) {
            return ((double) value.get()) / count.get();
        }
        return 0.0;
    }

    /**
     * Adds the specified value and count to existing value and count of the
     * measure
     *
     * @param value
     *            the value to be added
     * @param count
     *            the count to be incremented
     * @return self
     */
    public Average add(long value, long count) {
        this.value.addAndGet(value);
        this.count.addAndGet(count);
        return this;
    }

    /**
     * Adds the specified value to value and increments the count by 1
     *
     * @param value
     *            the value to be added
     * @return self
     */
    public Average add(long value) {
        return add(value, 1);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.wookler.server.common.AbstractMeasure#add(com.wookler.server.common
     * .AbstractMeasure)
     */
    @Override
    public AbstractMeasure add(AbstractMeasure measure) {
        if (measure instanceof Average) {
            add(((Average) measure).value(), ((Average) measure).count());
        }
        return this;
    }

    @Override
    public AbstractMeasure clear() {
        this.value.set(0);
        this.count.set(0);

        return this;
    }

    /**
     * String representation of average measure. Consists of
     * {@link AbstractMeasure} impl class name, time window, average, total
     * value, total count
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return String.format("{%s: WINDOW=%s, AVERAGE=%f, VALUE=%d, COUNT=%d}", getClass()
                        .getSimpleName(), new DateTime(window).toString(_WINDOW_DATE_FORMAT_), average(),
                value.get(), count.get());
    }

    @Override
    public AbstractMeasure copy() {
        Average m = new Average();
        m.window = window;
        m.value = new AtomicLong(value());
        m.count = new AtomicLong(count());

        return m;
    }
}
