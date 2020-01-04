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

package com.codekutter.common.stores;

import com.codekutter.common.AbstractState;

public class ConnectionState extends AbstractState<EConnectionState> {
    public ConnectionState() {
        setState(EConnectionState.Unknown);
    }

    public boolean isOpen() {
        return (getState() == EConnectionState.Open);
    }

    public boolean isClosed() {
        return (getState() == EConnectionState.Closed);
    }

    public void checkOpened() {
        if (!isOpen()) {
            throw new RuntimeException(String.format("Connection isn't open. [state=%s]", getState().name()));
        }
    }
}