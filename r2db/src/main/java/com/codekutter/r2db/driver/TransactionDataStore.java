package com.codekutter.r2db.driver;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class TransactionDataStore<C, T> extends AbstractDataStore<C> {
    private T transaction;

    public abstract void beingTransaction() throws DataStoreException;

    public abstract void commit() throws DataStoreException;

    public abstract void rollback() throws DataStoreException;
}
