package com.codekutter.common.stores.model;

import com.codekutter.common.Context;
import com.codekutter.common.model.CopyException;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.ValidationExceptions;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Getter
@Setter
@Entity
@Table(name = "tb_product")
public class Product implements IEntity<String> {
    @Id
    @Column(name = "product_id")
    private String id;
    @Column(name = "product_name")
    private String name;
    @Column(name = "description")
    private String description;
    @Column(name = "base_price")
    private double basePrice;
    @Column(name = "created_date")
    private long createdDate;

    @Override
    public String getKey() {
        return id;
    }

    @Override
    public int compare(String key) {
        return id.compareTo(key);
    }

    @Override
    public IEntity<String> copyChanges(IEntity<String> source, Context context) throws CopyException {
        return null;
    }

    @Override
    public IEntity<String> clone(Context context) throws CopyException {
        return null;
    }

    @Override
    public void validate() throws ValidationExceptions {

    }
}
