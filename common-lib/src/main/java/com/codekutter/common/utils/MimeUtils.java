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

package com.codekutter.common.utils;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.tika.Tika;
import org.apache.tika.langdetect.OptimaizeLangDetector;
import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.language.detect.LanguageResult;
import org.elasticsearch.common.Strings;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;

public class MimeUtils {
    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class FileMetaData {
        private String mimeType;
        private String language;
    }

    private static final Tika __tika = new Tika();
    private static final LanguageDetector __language = new OptimaizeLangDetector().loadModels();

    public static String getMimeType(@Nonnull File file) throws IOException {
        return __tika.detect(file);
    }

    public static String getMimeType(@Nonnull String path) throws IOException {
        return __tika.detect(path);
    }

    public static String getLanguage(@Nonnull String path) throws IOException {
        File file = new File(path);
        return getLanguage(file);
    }

    public static String getLanguage(@Nonnull File file) throws IOException {
        try {
            String content = IOUtils.readContent(file);
            if (content != null && !Strings.isNullOrEmpty(content)) {
                LanguageResult result = __language.detect(content);
                if (result != null) {
                    return result.getLanguage();
                }
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
        return null;
    }

    public static FileMetaData getMetadata(@Nonnull File file) throws IOException {
        FileMetaData metaData = new FileMetaData();
        metaData.mimeType = getMimeType(file);
        metaData.language = getLanguage(file);
        return metaData;
    }

    public static FileMetaData getMetadata(@Nonnull String path) throws IOException {
        File file = new File(path);
        return getMetadata(file);
    }
}
