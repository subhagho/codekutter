/*
 *  Copyright (2020) Subhabrata Ghosh (subho dot ghosh at outlook dot com)
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

package com.codekutter.common.stores.impl;

import com.codekutter.common.stores.annotations.IShardProvider;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public class DefaultShardProvider implements IShardProvider {
    private int shardCount;

    @Override
    public int getShard(Object key) {
        Preconditions.checkArgument(key instanceof String);
        Preconditions.checkArgument(!Strings.isNullOrEmpty((String) key));
        int value = 0;
        for (char c : ((String) key).toCharArray()) {
            value += c;
        }
        return value % shardCount;
    }
}
