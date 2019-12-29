package com.codekutter.r2db.driver;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class TransactionDataSource<C, T> extends AbstractDataSource<C> {
    private T transaction;

    public abstract void beingTransaction() throws DataSourceException;

    public abstract void commit() throws DataSourceException;

    public abstract void rollback() throws DataSourceException;
}
