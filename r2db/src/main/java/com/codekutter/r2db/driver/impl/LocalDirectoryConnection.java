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

package com.codekutter.r2db.driver.impl;

import com.codekutter.common.stores.AbstractConnection;
import com.codekutter.common.stores.EConnectionState;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;

@Getter
@Setter
@Accessors(fluent = true)
public class LocalDirectoryConnection extends AbstractConnection<File> {
    @ConfigAttribute(required = true)
    private String directory;
    @Setter(AccessLevel.NONE)
    private File dir;

    @Override
    public File connection() {
        return dir;
    }

    @Override
    public boolean hasTransactionSupport() {
        return false;
    }

    /**
     * Configure this type instance.
     *
     * @param node - Handle to the configuration node.
     * @throws ConfigurationException
     */
    @Override
    public void configure(@Nonnull AbstractConfigNode node) throws ConfigurationException {
        Preconditions.checkArgument(node instanceof ConfigPathNode);
        try {
            ConfigurationAnnotationProcessor.readConfigAnnotations(getClass(), (ConfigPathNode) node, this);
            dir = new File(directory);
            if (!dir.exists()) {
                throw new ConfigurationException(String.format("Specified directory source not found. [path=%s]", dir.getAbsolutePath()));
            }
            if (!dir.isDirectory()) {
                throw new ConfigurationException(String.format("Specified path isn't a directory. [path=%s]", dir.getAbsolutePath()));
            }
            state().setState(EConnectionState.Open);
        } catch (Exception ex) {
            state().setError(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public void close() throws IOException {
        // Do nothing
    }
}
