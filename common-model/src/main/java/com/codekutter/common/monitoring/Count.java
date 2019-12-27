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

import org.joda.time.DateTime;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple counter measure implementation of {@link AbstractMeasure}. Consists of
 * count value
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 05/08/14
 */
public class Count extends AbstractMeasure {
    /** counter value */
    protected AtomicLong value = new AtomicLong(0);

    /**
     * Get the current count value.
     *
     * @return - Count.
     */
    public long value() {
        return value.get();
    }

    /**
     * Set the count value.
     *
     * @param value
     *            - Count value.
     * @return - self.
     */
    public Count value(long value) {
        this.value.addAndGet(value);
        return this;
    }

    /**
     * Increment the count value by 1.
     *
     * @return - self.
     */
    public Count increment() {
        this.value.incrementAndGet();
        return this;
    }

    /**
     * Increment the current count value by the value of the specified count
     * measure.
     *
     * @param measure
     *            - Target measure to add.
     * @return - self.
     */
    @Override
    public AbstractMeasure add(AbstractMeasure measure) {
        if (measure instanceof Count) {
            value(((Count) measure).value());
        }
        return this;
    }

    /**
     * Clear the count value (set to 0).
     *
     * @return - self.
     */
    @Override
    public AbstractMeasure clear() {
        this.value.set(0);
        return this;
    }

    /**
     * Default to string representation of this count measure. Consists of Count
     * class name, window and count value
     *
     * @return - String representation.
     */
    @Override
    public String toString() {
        return String.format("{%s: WINDOW=%s, VALUE=%d}", getClass().getSimpleName(), new DateTime(
                window).toString(_WINDOW_DATE_FORMAT_), value.get());
    }

    /**
     * Create a copy/clone of this instance of the Count measure.
     *
     * @return - Count copy.
     */
    @Override
    public AbstractMeasure copy() {
        Count m = new Count();
        m.window = window;
        m.value = new AtomicLong(value());

        return m;
    }
}
