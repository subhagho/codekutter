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

package com.codekutter.r2db.driver.model;

import com.codekutter.common.model.IEntity;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.net.URI;

public abstract class RemoteFileEntity<K, C> extends File implements IEntity<K> {
    public RemoteFileEntity(String pathname) {
        super(pathname);
    }

    public RemoteFileEntity(String parent, String child) {
        super(parent, child);
    }

    public RemoteFileEntity(File parent, String child) {
        super(parent, child);
    }

    public RemoteFileEntity(URI uri) {
        super(uri);
    }

    public abstract String getRemotePath() throws IOException;

    public abstract boolean remoteExists() throws IOException;

    public abstract boolean remoteDelete() throws IOException;

    public abstract boolean isRemoteDirectory() throws IOException;

    public abstract boolean isRemoteFile() throws IOException;

    public abstract boolean canReadRemote(@Nonnull String user) throws IOException;

    public abstract boolean canWriteRemote(@Nonnull String user) throws IOException;

    public abstract boolean canExecuteRemote(@Nonnull String user) throws IOException;

    public abstract File copyToLocal() throws IOException;

    public abstract String copyToRemote() throws IOException;
}
