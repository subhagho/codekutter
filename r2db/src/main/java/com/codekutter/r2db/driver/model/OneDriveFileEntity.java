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

import com.codekutter.common.Context;
import com.codekutter.common.model.CopyException;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.ValidationExceptions;
import com.codekutter.common.utils.LogUtils;
import com.google.common.base.Preconditions;
import com.microsoft.graph.concurrency.ChunkedUploadProvider;
import com.microsoft.graph.concurrency.IProgressCallback;
import com.microsoft.graph.core.ClientException;
import com.microsoft.graph.models.extensions.*;
import com.microsoft.graph.options.Option;
import com.microsoft.graph.options.QueryOption;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.elasticsearch.common.Strings;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.*;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;

@Getter
@Setter
@Accessors(fluent = true)
public class OneDriveFileEntity extends RemoteFileEntity<OneDriveFileKey, IGraphServiceClient> {
    private static final String KEY_TARGET_FORMAT = "format";
    private static final String TARGET_FORMAT_PDF = "pdf";

    @Setter(AccessLevel.NONE)
    private OneDriveFileKey key;
    @Setter(AccessLevel.NONE)
    private FileState state = new FileState();
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    private IGraphServiceClient client;
    @Setter(AccessLevel.NONE)
    private DriveItem driveItem;
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    private ReentrantLock syncLock = new ReentrantLock();

    public OneDriveFileEntity(String pathname, String id, String remotePath) {
        super(pathname);
        key = new OneDriveFileKey(id, remotePath);
    }

    public OneDriveFileEntity(String parent, String child, String id, String remotePath) {
        super(parent, child);
        key = new OneDriveFileKey(id, remotePath);
    }

    public OneDriveFileEntity(File parent, String child, String id, String remotePath) {
        super(parent, child);
        key = new OneDriveFileKey(id, remotePath);
    }

    public OneDriveFileEntity(URI uri, String id, String remotePath) {
        super(uri);
        key = new OneDriveFileKey(id, remotePath);
    }

    public OneDriveFileEntity withClient(@Nonnull IGraphServiceClient client) {
        this.client = client;
        return this;
    }

    @Override
    public String getRemotePath() throws IOException {
        return key.path();
    }

    @Override
    public boolean remoteExists() throws IOException {
        if (client == null) {
            throw new IOException("Client handle not specified.");
        }
        if (!Strings.isNullOrEmpty(key.id())) {
            driveItem = client.me().drive().items(key.id()).buildRequest().get();
            if (driveItem != null) {
                state.setState(ESyncStatus.Synced);
                return true;
            } else {
                key.id(null);
            }
        } else if (!Strings.isNullOrEmpty(key.path())) {
            String fn = getName();
            String k = key.path().replace(fn, "");
            String pid = getFolderId(k, false);
            if (!Strings.isNullOrEmpty(pid)) {
                driveItem = client.me().drive().items(pid).itemWithPath(fn).buildRequest().get();
                if (driveItem != null) {
                    state.setState(ESyncStatus.Synced);
                    return true;
                } else {
                    key.id(null);
                }
            }
        }
        return false;
    }

    @Override
    public boolean remoteDelete() throws IOException {
        if (!Strings.isNullOrEmpty(key.id()) && client != null) {
            client.me().drive().items(key.id()).buildRequest().delete();
            key.id(null);
            state.setState(ESyncStatus.Deleted);
            return true;
        }
        return false;
    }

    @Override
    public boolean isRemoteDirectory() throws IOException {
        if (driveItem == null) {
            if (!remoteExists()) {
                return false;
            }
        }
        if (driveItem != null)
            return (driveItem.folder != null);
        return false;
    }

    @Override
    public boolean isRemoteFile() throws IOException {
        if (driveItem == null) {
            if (!remoteExists()) {
                return false;
            }
        }
        if (driveItem != null)
            return (driveItem.file != null);
        return false;
    }

    @Override
    public boolean canReadRemote(@Nonnull String user) throws IOException {
        return true;
    }

    @Override
    public boolean canWriteRemote(@Nonnull String user) throws IOException {
        return true;
    }

    @Override
    public boolean canExecuteRemote(@Nonnull String user) throws IOException {
        return true;
    }

    @Override
    public File copyToLocal() throws IOException {
        if (remoteExists()) {
            if (isRemoteDirectory()) {
                throw new IOException(String.format("Cannot download directory. [key=%s]", key.path()));
            }
            return download();
        } else {
            throw new IOException(String.format("Remote file not found. [path=%s]", key.path()));
        }
    }

    @Override
    public String copyToRemote() throws IOException {
        if (!exists()) {
            throw new IOException(String.format("File not found. [path=%s]", getAbsolutePath()));
        }
        if (isDirectory()) {
            return getFolderId(key.path(), true);
        } else {
            String fn = getName();
            String k = key.path().replace(fn, "");
            String pid = getFolderId(k, true);
            if (Strings.isNullOrEmpty(pid)) {
                throw new IOException(String.format("Error creating destination folder. [path=%s]", k));
            }
            try {
                UploadCallback callback = new UploadCallback(this);
                upload(pid, callback);
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        }
        return null;
    }

    public File convertToPdf() throws IOException {
        if (!isRemoteFile()) {
            throw new IOException(String.format("Cannot convert to PDF. [path=%s]", key.path()));
        }
        return downloadPDF();
    }

    private File downloadPDF() throws IOException {
        Preconditions.checkState(state.getState() != ESyncStatus.Unknown);
        syncLock.lock();
        try {
            if (!state.canSync()) {
                throw new IOException(String.format("Invalid State: Cannot sync. [key=%s][state=%s]", key.stringKey(), state.getState().name()));
            }

            state.setState(ESyncStatus.Downloading);
            String outfile = String.format("%s/%s.%s", getParentFile().getAbsolutePath(), getName(), TARGET_FORMAT_PDF);
            File outf = new File(outfile);
            if (outf.exists()) {
                if (!outf.delete()) {
                    throw new IOException(String.format("Error deleting file. [path=%s]", outf.getAbsolutePath()));
                }
            }
            LinkedList<Option> requestOptions = new LinkedList<>();
            requestOptions.add(new QueryOption(KEY_TARGET_FORMAT, TARGET_FORMAT_PDF));
            InputStream stream = client.me().drive().items(key.id()).content().buildRequest(requestOptions).get();
            if (stream != null) {
                try (
                        ReadableByteChannel remoteChannel = Channels
                                .newChannel(stream)) {
                    try (FileOutputStream fos = new FileOutputStream(outf)) {
                        long size = fos.getChannel()
                                .transferFrom(remoteChannel, 0, Long.MAX_VALUE);
                        LogUtils.debug(getClass(), String.format("Downloaded to local file. [path=%s][size=%d]", outf.getAbsolutePath(), size));
                    }
                    return outf;
                }
            }
        } finally {
            syncLock.unlock();
        }
        return null;
    }

    private File download() throws IOException {
        Preconditions.checkState(state.getState() != ESyncStatus.Unknown);
        syncLock.lock();
        try {
            if (!state.canSync()) {
                throw new IOException(String.format("Invalid State: Cannot sync. [key=%s][state=%s]", key.stringKey(), state.getState().name()));
            }
            if (exists()) {
                if (!delete()) {
                    throw new IOException(String.format("Error deleting local file. [path=%s]", getAbsolutePath()));
                }
            }
            state.setState(ESyncStatus.Downloading);
            InputStream stream = client.me().drive().items(key.id()).content().buildRequest().get();
            if (stream != null) {
                try (
                        ReadableByteChannel remoteChannel = Channels
                                .newChannel(stream)) {
                    try (FileOutputStream fos = new FileOutputStream(getAbsolutePath())) {
                        long size = fos.getChannel()
                                .transferFrom(remoteChannel, 0, Long.MAX_VALUE);
                        LogUtils.debug(getClass(), String.format("Downloaded to local file. [path=%s][size=%d]", getAbsolutePath(), size));
                    }
                    return new File(getAbsolutePath());
                }
            }
        } finally {
            syncLock.unlock();
        }
        return null;
    }

    private void upload(String folderId, IProgressCallback<DriveItem> callback) throws IOException {
        Preconditions.checkState(state.getState() != ESyncStatus.Unknown);
        syncLock.lock();
        try {
            if (!state.canSync()) {
                throw new IOException(String.format("Invalid State: Cannot sync. [key=%s][state=%s]", key.stringKey(), state.getState().name()));
            }
            state.setState(ESyncStatus.Uploading);
            UploadSession uploadSession = client
                    .me()
                    .drive()
                    .items(folderId)
                    .itemWithPath(getName())
                    .createUploadSession(new DriveItemUploadableProperties())
                    .buildRequest()
                    .post();

            ChunkedUploadProvider<DriveItem> chunkedUploadProvider = new ChunkedUploadProvider<>(
                    uploadSession,
                    client,
                    new FileInputStream(this),
                    length(),
                    DriveItem.class);

            chunkedUploadProvider.upload(callback);
        } finally {
            syncLock.unlock();
        }
    }

    private String getFolderId(String path, boolean create) throws IOException {
        Preconditions.checkState(client != null);
        String[] parts = path.split("/");
        DriveItem item = null;
        DriveItem parent = null;
        String pp = null;
        for (String part : parts) {
            if (Strings.isNullOrEmpty(part)) continue;
            if (pp == null) {
                pp = part;
            } else {
                pp = String.format("%s/%s", pp, part);
            }
            item = client.me().drive().root().itemWithPath(pp).buildRequest().get();
            if (item == null) {
                if (create) {
                    item = new DriveItem();
                    item.name = part;
                    Folder folder = new Folder();
                    item.folder = folder;
                    if (parent == null) {
                        item = client.me().drive().root().children().buildRequest().post(item);
                    } else {
                        item = client.me().drive().items(parent.id).children().buildRequest().post(item);
                    }
                } else {
                    return null;
                }
            }
            parent = item;
        }
        if (item != null) {
            return item.id;
        }
        return null;
    }

    /**
     * Compare the entity key with the key specified.
     *
     * @param key - Target Key.
     * @return - Comparision.
     */
    @Override
    public int compare(OneDriveFileKey key) {
        return this.key.compareTo(key);
    }

    /**
     * Copy the changes from the specified source entity
     * to this instance.
     * <p>
     * All properties other than the Key will be copied.
     * Copy Type:
     * Primitive - Copy
     * String - Copy
     * Enum - Copy
     * Nested Entity - Copy Recursive
     * Other Objects - Copy Reference.
     *
     * @param source  - Source instance to Copy from.
     * @param context - Execution context.
     * @return - Copied Entity instance.
     * @throws CopyException
     */
    @Override
    public IEntity<OneDriveFileKey> copyChanges(IEntity<OneDriveFileKey> source, Context context) throws CopyException {
        return null;
    }

    /**
     * Clone this instance of Entity.
     *
     * @param context - Clone Context.
     * @return - Cloned Instance.
     * @throws CopyException
     */
    @Override
    public IEntity<OneDriveFileKey> clone(Context context) throws CopyException {
        return null;
    }

    /**
     * Get the object instance Key.
     *
     * @return - Key
     */
    @Override
    public OneDriveFileKey getKey() {
        return key;
    }

    /**
     * Validate this entity instance.
     *
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    @Override
    public void validate() throws ValidationExceptions {

    }

    public static class UploadCallback implements IProgressCallback<DriveItem> {
        private OneDriveFileEntity entity;

        public UploadCallback(@Nonnull OneDriveFileEntity entity) {
            this.entity = entity;
        }

        /**
         * How progress updates are handled for this callback
         *
         * @param current the current amount of progress
         * @param max     the max amount of progress
         */
        @Override
        public void progress(long current, long max) {
            long pct = (current * 100) / max;
            if (pct % 10 == 0) {
                LogUtils.debug(getClass(), String.format("[key=%s]Uploaded [%d]pct...", entity.key.stringKey(), pct));
            }
        }

        /**
         * How successful results are handled
         *
         * @param driveItem the result
         */
        @Override
        public void success(DriveItem driveItem) {
            entity.driveItem = driveItem;
            entity.key.id(driveItem.id);
            entity.state.setState(ESyncStatus.Synced);
            LogUtils.debug(getClass(), String.format("Uploaded file. [id=%s][path=%s]", driveItem.id, entity.key.path()));
        }

        /**
         * How failures are handled
         *
         * @param ex the exception
         */
        @Override
        public void failure(ClientException ex) {
            entity.state.setState(ESyncStatus.Synced);
            LogUtils.error(getClass(), String.format("File upload failed. [key=%s][error=%s]", entity.key.stringKey(), ex.getLocalizedMessage()));
            LogUtils.error(getClass(), ex);
        }
    }
}
