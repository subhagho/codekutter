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

import com.codekutter.common.Context;
import com.codekutter.common.model.CopyException;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.ValidationExceptions;
import com.codekutter.r2db.driver.impl.annotations.Indexed;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;

@Getter
@Setter
@Entity
@Table(name = "tb_product")
@Indexed(index = "product")
public class Product implements IEntity<ProductKey> {
    @EmbeddedId
    private ProductKey id;
    @Column(name = "product_name")
    private String name;
    @Column(name = "description")
    private String description;
    @Column(name = "base_price")
    private double basePrice;
    @Column(name = "created_date")
    private long createdDate;

    @Override
    public ProductKey getKey() {
        return id;
    }


    @Override
    public int compare(ProductKey key) {
        return id.compareTo(key);
    }

    @Override
    public IEntity<ProductKey> copyChanges(IEntity<ProductKey> source, Context context) throws CopyException {
        return null;
    }

    @Override
    public IEntity<ProductKey> clone(Context context) throws CopyException {
        return null;
    }

    @Override
    public void validate() throws ValidationExceptions {

    }
}
