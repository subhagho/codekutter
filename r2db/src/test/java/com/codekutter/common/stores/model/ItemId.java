package com.codekutter.common.stores.model;

import com.codekutter.common.model.IKey;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;

@Getter
@Setter
@ToString
@Embeddable
public class ItemId implements IKey, Serializable {
    @Column(name = "order_id")
    private String orderId;
    @Column(name = "product_id")
    private String productId;

    public int compareTo(ItemId key) {
        int ret = orderId.compareTo(key.orderId);
        if (ret == 0) {
            ret = productId.compareTo(key.productId);
        }
        return ret;
    }

    /**
     * Get the String representation of the key.
     *
     * @return - Key String
     */
    @Override
    public String stringKey() {
        return String.format("%s::%s", orderId, productId);
    }
}
