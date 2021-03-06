/*
 *  Copyright (2020) Subhabrata Ghosh (subho dot ghosh at outlook dot com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.codekutter.zconfig.common;

import com.codekutter.common.StateException;
import com.codekutter.common.utils.*;
import com.codekutter.zconfig.common.model.Configuration;
import com.codekutter.zconfig.common.model.EncryptedValue;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.codekutter.zconfig.common.model.nodes.ConfigValueNode;
import com.codekutter.zconfig.common.model.nodes.EValueType;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

@Getter
@Setter
public abstract class BaseConfigEnv {
    private static final String CONFIG_PATH_NODE_TEMP = "__TMP__";

    /**
     * Environment Instance Lock.
     */
    private static ReentrantLock _envLock = new ReentrantLock();
    private static BaseConfigEnv __env = null;
    @Setter(AccessLevel.NONE)
    protected final EnvState state = new EnvState();
    @Setter(AccessLevel.NONE)
    protected final String configName;
    @Setter(AccessLevel.NONE)
    private final ICryptoHandler defaultCryptoHandler = new ConfigCryptoHandler();
    @Setter(AccessLevel.PACKAGE)
    protected Configuration configuration;
    @ConfigAttribute(name = "cryptoHandler")
    private Class<? extends ICryptoHandler> cryptoHandlerClass;
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    private final Map<Long, ManagedThread> managedThreads = new HashMap<>();

    protected BaseConfigEnv(@Nonnull String configName) {
        this.configName = configName;
    }

    public static synchronized EncryptedValue createEncryptedNode(@Nonnull String encryptedValue) throws ConfigurationException {
        if (__env.configuration == null) {
            throw new ConfigurationException("Configuration not initialized...");
        }
        AbstractConfigNode tnode = __env.configuration.getRootConfigNode().find(CONFIG_PATH_NODE_TEMP);
        if (tnode == null) {
            ConfigPathNode pnode = new ConfigPathNode(__env.configuration, __env.configuration.getRootConfigNode());
            __env.configuration.getRootConfigNode().addChildNode(pnode);
            tnode = pnode;
        }
        if (!(tnode instanceof ConfigPathNode)) {
            throw new ConfigurationException(String.format("Invalid temp node. [type=%s]", tnode.getClass().getCanonicalName()));
        }
        ConfigValueNode vnode = new ConfigValueNode(__env.configuration, tnode);
        vnode.setName(UUID.randomUUID().toString());
        vnode.setValueType(EValueType.STRING);
        vnode.setValue(encryptedValue);
        ((ConfigPathNode) tnode).addChildNode(vnode);
        EncryptedValue ev = new EncryptedValue(vnode);
        LogUtils.debug(BaseConfigEnv.class, ev);
        return ev;
    }

    /**
     * Shutdown this client environment.
     */
    public static void shutdown() {
        try {
            if (__env != null) {
                getEnvLock();
                try {
                    __env.dispose();
                    __env = null;
                } finally {
                    releaseEnvLock();
                }
            }
        } catch (Exception ex) {
            LogUtils.error(BaseConfigEnv.class, ex);
        }
    }

    /**
     * Get the env initialization lock.
     *
     * @throws EnvException - Exception raised if Env has already been disposed.
     */
    protected static void getEnvLock() throws EnvException {
        if (__env != null && __env.state.getState() == EEnvState.Disposed) {
            throw new EnvException("Environment has already been disposed.");
        }
        _envLock.lock();
    }

    /**
     * Release the env initialization lock.
     *
     * @throws EnvException - Exception raised if current thread doesn't hold the lock.
     */
    protected static void releaseEnvLock() throws EnvException {
        if (__env != null && __env.state.getState() == EEnvState.Disposed) {
            throw new EnvException("Environment has already been disposed.");
        }
        if (_envLock.isLocked() && _envLock.isHeldByCurrentThread()) {
            _envLock.unlock();
        } else {
            throw new EnvException(String.format(
                    "Lock not acquired or held by another thread. [thread id=%d]",
                    Thread.currentThread().getId()));
        }
    }

    /**
     * Initialize the ENV handle.
     *
     * @param type - Type of the env instance.
     * @return - Created Env handle.
     * @throws EnvException - Exception raised if initialization lock not acquired by current thread.
     */
    @SuppressWarnings("unchecked")
    protected static BaseConfigEnv initialize(@Nonnull Class<? extends BaseConfigEnv> type, @Nonnull String configName)
            throws EnvException {
        if (!_envLock.isLocked() || !_envLock.isHeldByCurrentThread()) {
            throw new EnvException("Environment not locked for initialisation.");
        }
        try {
            if (__env == null) {
                Constructor<? extends BaseConfigEnv> ctor = (Constructor<? extends BaseConfigEnv>) ReflectionUtils.getConstructor(type, String.class);
                __env = ctor.newInstance(configName);
                LogUtils.info(BaseConfigEnv.class,
                        String.format("Created ENV instance with type [%s]...",
                                type.getCanonicalName()));
            }
            return __env;
        } catch (Exception ex) {
            throw new EnvException(ex);
        }
    }

    /**
     * Get a handle to the client environment singleton.
     *
     * @return - Environment handle.
     * @throws EnvException
     */
    public static <T extends BaseConfigEnv> T env() throws EnvException {
        try {
            if (__env != null)
                __env.checkState(EEnvState.Initialized);
            return (T) __env;
        } catch (StateException e) {
            throw new EnvException(e);
        }
    }

    public ICryptoHandler cryptoHandler() throws CryptoException {
        try {
            if (cryptoHandlerClass != null) {
                return cryptoHandlerClass.newInstance();
            }
            return defaultCryptoHandler;
        } catch (Exception ex) {
            throw new CryptoException(ex);
        }
    }

    public synchronized ManagedThread create(@Nonnull String name, @Nonnull Runnable runnable) {
        ManagedThread thread = new ManagedThread(runnable, name);
        managedThreads.put(thread.getId(), thread);
        return thread;
    }

    public synchronized ManagedThread create(@Nonnull String name, @Nonnull Runnable runnable, @Nonnull ThreadGroup group) {
        ManagedThread thread = new ManagedThread(group, runnable, name);
        managedThreads.put(thread.getId(), thread);
        return thread;
    }

    public synchronized boolean remove(@Nonnull ManagedThread thread) {
        if (managedThreads.containsKey(thread.getId())) {
            return managedThreads.remove(thread.getId()) != null;
        }
        return false;
    }

    /**
     * Perform post-initialisation tasks if any.
     *
     * @throws ConfigurationException
     */
    public abstract void postInit() throws ConfigurationException;

    protected abstract void dispose();

    /**
     * Check the state of this instance.
     *
     * @param state - Expected state.
     * @throws StateException - Exception will be raised if state is not as expected.
     */
    protected void checkState(@Nonnull EEnvState state) throws StateException {
        Preconditions.checkArgument(state != null);
        this.state.checkState(state);
    }
}
