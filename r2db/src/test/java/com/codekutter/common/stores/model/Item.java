package com.codekutter.common.stores.model;

import com.codekutter.common.Context;
import com.codekutter.common.model.CopyException;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.ValidationExceptions;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

@Getter
@Setter
@Entity
@Table(name = "tb_order_items")
public class Item implements IEntity<ItemId> {
    @EmbeddedId
    private ItemId id;
    private int quantity;
    private double unitPrice;

    @Override
    public ItemId getKey() {
        return id;
    }

    @Override
    public int compare(ItemId key) {
        return id.compareTo(key);
    }

    @Override
    public IEntity<ItemId> copyChanges(IEntity<ItemId> source, Context context) throws CopyException {
        return null;
    }

    @Override
    public IEntity<ItemId> clone(Context context) throws CopyException {
        return null;
    }

    @Override
    public void validate() throws ValidationExceptions {

    }
}
