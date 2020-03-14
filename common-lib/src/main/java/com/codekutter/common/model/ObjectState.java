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

package com.codekutter.common.model;

import com.codekutter.common.AbstractState;
import com.codekutter.common.StateException;

public class ObjectState extends AbstractState<EObjectState> {
    public ObjectState() {
        setState(EObjectState.Unknown);
    }

    public void check(EObjectState expected, Class<?> caller) throws StateException {
        if (getState() != expected) {
            throw new StateException(String.format("[%s] Invalid Object state. [expected=%s][current=%s]", caller.getCanonicalName(), expected.name(), getState().name()));
        }
    }
}
