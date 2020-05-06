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

package com.codekutter.common.stores.model;

import com.codekutter.common.model.IKey;
import com.codekutter.common.utils.CommonUtils;
import com.codekutter.r2db.driver.model.Searchable;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import javax.persistence.Column;
import javax.persistence.Embeddable;

@Getter
@Setter
@Embeddable
public class CustomerKey implements IKey {
    @Searchable(faceted = true)
    @Column(name = "customer_id")
    private String key;

    public CustomerKey() {
    }

    public CustomerKey(@Nonnull String key) {
        this.key = key;
    }

    /**
     * Get the String representation of the key.
     *
     * @return - Key String
     */
    @Override
    public String stringKey() {
        return key;
    }

    /**
     * Compare the current key to the target.
     *
     * @param key - Key to compare to
     * @return - == 0, < -x, > +x
     */
    @Override
    public int compareTo(IKey key) {
        if (key instanceof CustomerKey) {
            String k = ((CustomerKey) key).key;
            return this.key.compareTo(k);
        }
        return -1;
    }

    @Override
    public int hashCode() {
        return CommonUtils.getHashCode(stringKey());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CustomerKey) {
            return compareTo((IKey) obj) == 0;
        }
        return super.equals(obj);
    }
}
