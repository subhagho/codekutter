package com.codekutter.common.model;

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

    public int compareTo(StringKey source) {
        return key.compareTo(source.key);
    }
}
