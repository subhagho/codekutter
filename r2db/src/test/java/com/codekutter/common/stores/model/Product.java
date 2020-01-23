package com.codekutter.common.stores.model;

import com.codekutter.common.Context;
import com.codekutter.common.model.CopyException;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.StringKey;
import com.codekutter.common.model.ValidationExceptions;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;

@Getter
@Setter
@Entity
@Table(name = "tb_product")
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
