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
import com.codekutter.common.auditing.Audited;
import com.codekutter.common.model.CopyException;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.ValidationExceptions;
import com.codekutter.common.stores.annotations.EJoinType;
import com.codekutter.common.stores.annotations.Encrypted;
import com.codekutter.common.stores.annotations.Reference;
import com.codekutter.r2db.driver.impl.annotations.Indexed;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import javax.persistence.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

@Getter
@Setter
@Entity
@Table(name = "tb_orders")
@Audited
@Indexed(index = "order_index")
@Encrypted
public class Order implements IEntity<OrderKey> {
    @EmbeddedId
    private OrderKey id;
    @ManyToOne(cascade = CascadeType.ALL)
    @JoinColumn(name = "customer_id")
    private Customer customer;
    @Transient
    @Reference(target = Item.class,
            type = EJoinType.One2Many,
            columns = @JoinColumns({@JoinColumn(name = "id.key", referencedColumnName = "id.orderId")}),
            query = "quantity > 0"
    )
    private List<Item> items;
    @Column(name = "order_amount")
    private double amount = 0;
    @Column(name = "created_date")
    private Date createdOn;
    @Transient
    @Encrypted
    private List<String> testEncryptionList;
    @Encrypted
    @Transient
    private Set<Customer> testSetEmbedded;

    public void addItem(@Nonnull Item item) {
        if (items == null) {
            items = new ArrayList<>();
        }
        items.add(item);
        amount += item.getQuantity() * item.getUnitPrice();
    }

    @Override
    public OrderKey getKey() {
        return id;
    }

    @Override
    public int compare(OrderKey key) {
        return id.compareTo(key);
    }

    @Override
    public IEntity<OrderKey> copyChanges(IEntity<OrderKey> source, Context context) throws CopyException {
        return null;
    }

    @Override
    public IEntity<OrderKey> clone(Context context) throws CopyException {
        return null;
    }

    @Override
    public void validate() throws ValidationExceptions {

    }
}
