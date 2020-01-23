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

package com.codekutter.common.model;

import com.codekutter.common.utils.CommonUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;

@Embeddable
@Getter
@Setter
@ToString
public class LockId implements IKey, Serializable {
    @Column(name = "namespace")
    private String namespace;
    @Column(name = "name")
    private String name;

    @Override
    public int hashCode() {
        return CommonUtils.getHashCode(String.format("%s::%s", namespace, name));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (this.hashCode() == obj.hashCode()) return true;
        if (obj instanceof LockId) {
            if ((namespace != null && namespace.compareTo(((LockId) obj).namespace) == 0) &&
                    (name != null && name.compareTo(((LockId) obj).name) == 0)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get the String representation of the key.
     *
     * @return - Key String
     */
    @Override
    public String stringKey() {
        return String.format("%s.%s", namespace, name);
    }

    /**
     * Compare the current key to the target.
     *
     * @param key - Key to compare to
     * @return - == 0, < -x, > +x
     */
    @Override
    public int compareTo(IKey key) {
        if (key instanceof LockId) {
            LockId l = (LockId) key;
            int ret = namespace.compareTo(l.namespace);
            if (ret == 0) {
                ret = name.compareTo(l.name);
            }
            return ret;
        }
        return -1;
    }
}
