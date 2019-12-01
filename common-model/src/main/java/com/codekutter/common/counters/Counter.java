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

package com.codekutter.common.counters;

import com.codekutter.common.TimeWindow;

import static com.codekutter.common.utils.LogUtils.*;

/**
 * Counter impl of {@link AbstractCounter}. The implementation of abstract
 * copy() consists of creating a new Counter instance as copy and returning this
 * copied instance.
 * 
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 05/08/14
 */
public class Counter extends AbstractCounter {

    /**
     * Instantiates a new counter with the specified window and instantiates
     * total, period and delta measures of specified type
     *
     * @param window
     *            the window
     * @param type
     *            the type
     */
    public Counter(TimeWindow window, Class<? extends AbstractMeasure> type) {
        super(window, type);
        try {
            total = type.newInstance();
            period = type.newInstance();
            delta = type.newInstance();
        } catch (Throwable t) {
            debug(getClass(), t);
        }
    }

    @Override
    public AbstractCounter copy() {
        Counter c = new Counter(windowdef, type);
        c.setNamespace(namespace);
        c.setName(name);
        c.id = id;
        c.mode = mode;
        c.window = window;

        c.total = total.copy();
        c.period = period.copy();
        c.delta = delta.copy();

        return c;
    }

}
