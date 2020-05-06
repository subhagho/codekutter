/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Copyright (c) $year
 * Date: 25/2/19 10:11 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.common.utils;

import com.codekutter.common.model.EReaderType;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Helper class to download/upload remote files.
 */
public class RemoteFileHelper {
    /**
     * Download the specified file from the remote location.
     *
     * @param remoteUri - URI of the HTTP endpoint to download from.
     * @param location  - Local File to create.
     * @return - Number of file bytes read.
     * @throws IOException
     */
    public static long downloadRemoteFile(@Nonnull URI remoteUri,
                                          @Nonnull File location)
            throws IOException {
        Preconditions.checkArgument(remoteUri != null);
        Preconditions.checkArgument(location != null);
        EReaderType type = EReaderType.parseFromUri(remoteUri);
        Preconditions.checkNotNull(type);

        if (type != EReaderType.HTTP && type != EReaderType.HTTPS) {
            throw new IOException(String.format(
                    "Method should be only called for HTTP(S) channel. [passed channel=%s]",
                    type.name()));
        }
        if (location.exists()) {
            if (!location.delete()) {
                throw new IOException(
                        String.format("Error deleting existing file : [path=%s]",
                                location.getAbsolutePath()));
            }
        }
        URL url = remoteUri.toURL();
        LogUtils.info(RemoteFileHelper.class,
                String.format("Downloading file [url=%s]", url.toString()));
        try (
                ReadableByteChannel remoteChannel = Channels
                        .newChannel(url.openStream())) {
            try (FileOutputStream fos = new FileOutputStream(location)) {
                return fos.getChannel()
                        .transferFrom(remoteChannel, 0, Long.MAX_VALUE);
            }
        }
    }

    /**
     * Download the directory content from a remote location. Directory content is
     * expected to be zipped.
     *
     * @param remoteUri - URI of the HTTP endpoint to download from.
     * @param directory - Local directory to write to.
     * @return - Byte read (zipfile).
     * @throws IOException
     */
    public static long downloadRemoteDirectory(@Nonnull URI remoteUri,
                                               @Nonnull File directory)
            throws IOException {
        String tempf = IOUtils.getTempFile();
        File file = new File(tempf);

        long bread = downloadRemoteFile(remoteUri, file);
        if (bread <= 0) {
            throw new IOException(
                    String.format("No data downloaded from URL. [uri=%s]",
                            remoteUri.toString()));
        }
        try {
            byte[] buffer = new byte[1024];
            try (
                    ZipInputStream zis = new ZipInputStream(
                            new FileInputStream(file))) {
                ZipEntry zipEntry = zis.getNextEntry();
                while (zipEntry != null) {
                    File newFile = newFile(directory, zipEntry);
                    FileOutputStream fos = new FileOutputStream(newFile);
                    int len;
                    while ((len = zis.read(buffer)) > 0) {
                        fos.write(buffer, 0, len);
                    }
                    fos.close();
                    zipEntry = zis.getNextEntry();
                }
                zis.closeEntry();
            }
            LogUtils.info(RemoteFileHelper.class,
                    String.format("Created downloaded directory. [path=%s]",
                            directory.getAbsolutePath()));
            return bread;
        } finally {
            file.delete();
        }
    }

    /**
     * Create a new file for the Zip entry in the destination folder.
     *
     * @param destinationDir - Destination folder.
     * @param zipEntry       - Zip Entry to process.
     * @return - Created output file.
     * @throws IOException
     */
    private static File newFile(File destinationDir, ZipEntry zipEntry)
            throws IOException {
        File destFile = new File(destinationDir, zipEntry.getName());

        String destDirPath = destinationDir.getCanonicalPath();
        String destFilePath = destFile.getCanonicalPath();

        if (!destFilePath.startsWith(destDirPath + File.separator)) {
            throw new IOException(
                    "Entry is outside of the target dir: " + zipEntry.getName());
        }

        return destFile;
    }
}
