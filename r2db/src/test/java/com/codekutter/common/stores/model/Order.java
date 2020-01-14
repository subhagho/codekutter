package com.codekutter.common.stores.model;

import com.codekutter.common.Context;
import com.codekutter.common.model.CopyException;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.StringKey;
import com.codekutter.common.model.ValidationExceptions;
import com.codekutter.common.stores.annotations.Reference;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import javax.persistence.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Getter
@Setter
@Entity
@Table(name = "tb_orders")
public class Order implements IEntity<StringKey> {
    @Id
    @Column(name = "order_id")
    private String id;
    @OneToOne
    @JoinColumn(name = "customer_id")
    private Customer customer;
    @Transient
    @Reference(target = Item.class,
            columns = @JoinColumns({@JoinColumn(name = "id", referencedColumnName = "id.orderId")}),
            query = "quantity > 0"
    )
    private List<Item> items;
    @Column(name = "order_amount")
    private double amount = 0;
    @Column(name = "created_date")
    private Date createdOn;

    public void addItem(@Nonnull Item item) {
        if (items == null) {
            items = new ArrayList<>();
        }
        items.add(item);
        amount += item.getQuantity() * item.getUnitPrice();
    }

    @Override
    public StringKey getKey() {
        return new StringKey(id);
    }

    @Override
    public int compare(StringKey key) {
        return id.compareTo(key.getKey());
    }

    @Override
    public IEntity<StringKey> copyChanges(IEntity<StringKey> source, Context context) throws CopyException {
        return null;
    }

    @Override
    public IEntity<StringKey> clone(Context context) throws CopyException {
        return null;
    }

    @Override
    public void validate() throws ValidationExceptions {

    }
}
