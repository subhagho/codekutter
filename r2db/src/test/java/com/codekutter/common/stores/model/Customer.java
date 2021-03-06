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
import com.codekutter.common.stores.annotations.Encrypted;
import com.codekutter.r2db.driver.impl.annotations.Indexed;
import com.codekutter.r2db.driver.model.Searchable;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;
import java.util.Date;

@Getter
@Setter
@Entity
@Table(name = "tb_customer")
@Indexed(index = "customer_index")
@Encrypted
public class Customer implements IEntity<CustomerKey> {
    @EmbeddedId
    private CustomerKey id;
    @Searchable(faceted = true)
    @Column(name = "first_name")
    private String firstName;
    @Column(name = "last_name")
    private String lastName;
    @Column(name = "date_of_birth")
    private Date dateOfBirth;
    @Encrypted
    @Column(name = "email_id")
    private String emailId;
    @Encrypted
    @Searchable(faceted = true)
    @Column(name = "phone_no")
    private String phoneNumber;

    @Override
    public CustomerKey getKey() {
        return id;
    }


    @Override
    public int compare(CustomerKey key) {
        return id.compareTo(key);
    }

    @Override
    public IEntity<CustomerKey> copyChanges(IEntity<CustomerKey> source, Context context) throws CopyException {
        return null;
    }

    @Override
    public IEntity<CustomerKey> clone(Context context) throws CopyException {
        return null;
    }

    @Override
    public void validate() throws ValidationExceptions {

    }
}
