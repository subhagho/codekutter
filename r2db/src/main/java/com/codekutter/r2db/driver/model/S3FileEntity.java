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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.codekutter.common.Context;
import com.codekutter.common.model.CopyException;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.ValidationExceptions;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.io.*;
import java.net.URI;

public class S3FileEntity extends RemoteFileEntity<S3FileKey, AmazonS3> {
    private static final int DEFAULT_BUFFER_SIZE = 8 * 1024;
    private S3FileKey key;
    private AmazonS3 client;

    public S3FileEntity(String bucket, String key, String pathname) {
        super(pathname);
        this.key = new S3FileKey(bucket, key);
    }

    public S3FileEntity(String parent, String child, String bucket, String key) {
        super(parent, child);
        this.key = new S3FileKey(bucket, key);
    }

    public S3FileEntity(File parent, String child, String bucket, String key) {
        super(parent, child);
        this.key = new S3FileKey(bucket, key);
    }

    public S3FileEntity(URI uri, String bucket, String key) {
        super(uri);
        this.key = new S3FileKey(bucket, key);
    }

    public S3FileEntity withClient(@Nonnull AmazonS3 client) {
        this.client = client;
        return this;
    }

    @Override
    public String getRemotePath() throws IOException {
        return key.key();
    }

    @Override
    public boolean remoteExists() throws IOException {
        if (client == null) {
            throw new IOException("AWS S3 Client not set.");
        }
        return client.doesObjectExist(key.bucket(), key.key());
    }

    @Override
    public boolean remoteDelete() throws IOException {
        if (client == null) {
            throw new IOException("AWS S3 Client not set.");
        }
        client.deleteObject(key.bucket(), key.key());
        return true;
    }

    @Override
    public boolean isRemoteDirectory() throws IOException {
        return false;
    }

    @Override
    public boolean isRemoteFile() throws IOException {
        return remoteExists();
    }

    @Override
    public boolean canReadRemote(@Nonnull String user) throws IOException {
        throw new IOException("Method not available.");
    }

    @Override
    public boolean canWriteRemote(@Nonnull String user) throws IOException {
        throw new IOException("Method not available.");
    }

    @Override
    public boolean canExecuteRemote(@Nonnull String user) throws IOException {
        throw new IOException("Method not available.");
    }

    @Override
    public File copyToLocal() throws IOException {
        if (client == null) {
            throw new IOException("AWS S3 Client not set.");
        }

        File dir = getParentFile();
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new IOException(String.format("Error creating parent folder. [path=%s]", dir.getAbsolutePath()));
            }
        } else if (exists()) {
            if (!delete()) {
                throw new IOException(String.format("Error deleting existing file. [path=%s]", getAbsolutePath()));
            }
        }
        S3Object source = client.getObject(key.bucket(), key.key());
        if (source == null) {
            throw new IOException(String.format("S3 Object not found. [bucket=%s][key=%s]", key.bucket(), key.bucket()));
        }
        try (InputStream reader = new BufferedInputStream(
                source.getObjectContent())) {
            try (FileOutputStream fos = new FileOutputStream(this)) {
                byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
                while (true) {
                    int size = reader.read(buffer, 0, DEFAULT_BUFFER_SIZE);
                    if (size <= 0) break;
                    fos.write(buffer, 0, size);
                    if (size < DEFAULT_BUFFER_SIZE) break;
                }
            }
        }
        return this;
    }

    @Override
    public String copyToRemote() throws IOException {
        if (client == null) {
            throw new IOException("AWS S3 Client not set.");
        }
        if (!exists()) {
            throw new IOException(String.format("File not found. [path=%s]", getAbsolutePath()));
        }
        try {
            PutObjectRequest request = new PutObjectRequest(key.bucket(), key.key(), this);
            client.putObject(request);
            return key.stringKey();
        } catch (AmazonServiceException ex) {
            throw new IOException(ex);
        } catch (SdkClientException ex) {
            throw new IOException(ex);
        }
    }

    /**
     * Get the unique Key for this entity.
     *
     * @return - Entity Key.
     */
    @Override
    public S3FileKey getKey() {
        return key;
    }

    /**
     * Compare the entity key with the key specified.
     *
     * @param key - Target Key.
     * @return - Comparision.
     */
    @Override
    public int compare(S3FileKey key) {
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
    public IEntity<S3FileKey> copyChanges(IEntity<S3FileKey> source, Context context) throws CopyException {
        Preconditions.checkArgument(source instanceof S3FileEntity);
        try {
            S3FileEntity sf = (S3FileEntity) source;
            File s = null;
            if (!sf.exists()) {
                if (client == null) {
                    if (!(context instanceof S3FileContext)) {
                        throw new CopyException("S3 Context not specified.");
                    }
                    client = ((S3FileContext) context).client();
                    if (client == null) {
                        throw new CopyException("S3 Client handle not specified in context.");
                    }
                }
                s = sf.copyToLocal();
            } else {
                s = new File(sf.getAbsolutePath());
            }
            try (FileInputStream fis = new FileInputStream(s)) {
                if (exists()) {
                    if (!delete()) {
                        throw new CopyException(String.format("Error deleting local file. [path=%s]", getAbsolutePath()));
                    }
                }
                try (FileOutputStream fos = new FileOutputStream(this)) {
                    byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
                    int offset = 0;
                    while (true) {
                        int size = fis.read(buffer, offset, DEFAULT_BUFFER_SIZE);
                        if (size <= 0) break;
                        fos.write(buffer, offset, size);
                        if (size < DEFAULT_BUFFER_SIZE) break;
                        offset += size;
                    }
                }
            }
            return this;
        } catch (Exception ex) {
            throw new CopyException(ex);
        }
    }

    /**
     * Clone this instance of Entity.
     *
     * @param context - Clone Context.
     * @return - Cloned Instance.
     * @throws CopyException
     */
    @Override
    public IEntity<S3FileKey> clone(Context context) throws CopyException {
        return new S3FileEntity(key.bucket(), key.key(), getAbsolutePath()).withClient(client);
    }

    /**
     * Validate this entity instance.
     *
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    @Override
    public void validate() throws ValidationExceptions {

    }
}
