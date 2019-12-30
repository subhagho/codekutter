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

import com.codekutter.common.model.IEntity;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@Accessors(fluent = true)
@ConfigPath(path = "connection")
public abstract class AbstractConnection<T> implements IConfigurable, Closeable {
    @Setter(AccessLevel.NONE)
    private ConnectionState state = new ConnectionState();
    @Setter(AccessLevel.NONE)
    @SuppressWarnings("rawtypes")
    private List<Class<? extends IEntity>> supportedTypes = new ArrayList<>();

    public AbstractConnection() {

    }

    public abstract T connection();

    public abstract boolean hasTransactionSupport();
}
