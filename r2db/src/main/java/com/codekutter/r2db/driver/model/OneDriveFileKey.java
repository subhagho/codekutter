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

package com.codekutter.r2db.driver.model;

import com.codekutter.common.model.IKey;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public class OneDriveFileKey implements IKey, Comparable<OneDriveFileKey> {
    private String id;
    private String path;

    public OneDriveFileKey() {}

    public OneDriveFileKey(String id, String path) {
        this.id = id;
        this.path = path;
    }

    /**
     * Get the String representation of the key.
     *
     * @return - Key String
     */
    @Override
    public String stringKey() {
        return String.format("%s::%s", id, path);
    }

    /**
     * Compare the current key to the target.
     *
     * @param key - Key to compare to
     * @return - == 0, < -x, > +x
     */
    @Override
    public int compareTo(IKey key) {
        if (key instanceof OneDriveFileKey) {
            OneDriveFileKey k = (OneDriveFileKey) key;
            return compareTo(k);
        }
        return -1;
    }

    @Override
    public int compareTo(OneDriveFileKey key) {
        if (key != null) {
            int ret = id.compareTo(key.id);
            if (ret == 0)
                ret = path.compareTo(key.path);
            return ret;
        }
        return -1;
    }
}
