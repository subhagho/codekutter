package com.codekutter.r2db.driver;

import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.utils.ReflectionUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

public class ConnectionManager implements IConfigurable {
    private Map<String, AbstractConnection<?>> connections = new HashMap<>();
    @SuppressWarnings("rawtypes")
    private Map<Class<? extends IEntity>, AbstractConnection<?>> entityIndex = new HashMap<>();

    @SuppressWarnings("unchecked")
    public <T> AbstractConnection<T> getConnection(@NonNull String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        if (connections.containsKey(name)) {
            return (AbstractConnection<T>) connections.get(name);
        }
        return null;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> AbstractConnection<T> getConnection(@NonNull Class<? extends IEntity> type, boolean checkSuperTypes) {
        Class<? extends IEntity> ct = type;
        while(true) {
            if (entityIndex.containsKey(ct)) {
                return (AbstractConnection<T>) entityIndex.get(ct);
            }
            if (checkSuperTypes) {
                Class<?> t = ct.getSuperclass();
                if (ReflectionUtils.implementsInterface(IEntity.class, t)) {
                    ct = (Class<? extends IEntity>) t;
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        return null;
    }

    @SuppressWarnings("rawtypes")
    public <T> AbstractConnection<T> getConnection(@NonNull Class<? extends IEntity> type) {
        return getConnection(type, false);
    }

    @Override
    public void configure(@Nonnull AbstractConfigNode node) throws ConfigurationException {

    }
}
