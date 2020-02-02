package com.codekutter.common.model;

import com.codekutter.common.utils.CommonUtils;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;

@Getter
@Setter
@Embeddable
public class StringKey implements IKey, Serializable {
    @Column(name = "key")
    private String key;

    public StringKey() {}
    public StringKey(@Nonnull String key) {
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
        if (key instanceof StringKey) {
            StringKey s = (StringKey)key;
            return this.key.compareTo(s.key);
        }
        return -1;
    }

    @Override
    public int hashCode() {
        return CommonUtils.getHashCode(stringKey());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof StringKey) {
            return compareTo((IKey) obj) == 0;
        }
        return super.equals(obj);
    }
}
