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

/**
 * Base class for defining measures stored for any counter.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 04/08/14
 */
public abstract class AbstractMeasure {
    /**
     * Default format for representing the timestamp.
     */
    public static final String _WINDOW_DATE_FORMAT_ = "yyyy-MM-dd HH:mm:ss.SSS";

    protected long window = System.currentTimeMillis();

    /**
     * Get the time window this measure is computed for.
     *
     * @return - Time window.
     */
    public long window() {
        return window;
    }

    /**
     * Set the time window this measure is for. This value represents the create of the time window.
     *
     * @param window - Start time window.
     * @return - self.
     */
    public AbstractMeasure window(long window) {
        this.window = window;

        return this;
    }

    /**
     * Abstract method to update the measure and add the value of the specified target.
     *
     * @param measure - Target measure to add.
     * @return - self.
     */
    public abstract AbstractMeasure add(AbstractMeasure measure);

    /**
     * Clear and reset the measure value.
     *
     * @return - self.
     */
    public abstract AbstractMeasure clear();

    /**
     * Create a copy/clone of this instance of the measure.
     *
     * @return - Created copy.
     */
    public abstract AbstractMeasure copy();
}
