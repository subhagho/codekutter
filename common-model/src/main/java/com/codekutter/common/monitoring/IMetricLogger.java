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

import com.codekutter.common.model.App;
import com.codekutter.zconfig.common.IConfigurable;

import java.text.ParseException;
import java.util.List;

public interface IMetricLogger extends IConfigurable {
    /**
     * Log the specified counter values.
     *
     * @param application     - Application information.
     * @param counters - List of counters to log.
     */
    void log(App application, List<AbstractCounter> counters);

    /**
     * Parse a counter value from the specified string.
     *
     * @param value - String input to parse counter from.
     * @return - Parsed counter.
     * @throws ParseException
     */
    AbstractCounter parse(String value) throws ParseException;
}
