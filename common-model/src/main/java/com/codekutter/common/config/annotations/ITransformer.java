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
 * Date: 5/3/19 8:32 AM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.common.config.annotations;

/**
 * Field transformation interface: to be implemented for providing field value
 * transformations.
 *
 * @param <T> - Target field type.
 * @param <S> - Source field type.
 */
public interface ITransformer<T, S> {
    public static class TransformationException extends Exception {
        private static final String PREFIX = "Transformation Error: %s";

        /**
         * Exception constructor with error message string.
         *
         * @param s - Error message string.
         */
        public TransformationException(String s) {
            super(String.format(PREFIX, s));
        }

        /**
         * Exception constructor with error message string and inner cause.
         *
         * @param s         - Error message string.
         * @param throwable - Inner cause.
         */
        public TransformationException(String s, Throwable throwable) {
            super(String.format(PREFIX, s), throwable);
        }

        /**
         * Exception constructor inner cause.
         *
         * @param throwable - Inner cause.
         */
        public TransformationException(Throwable throwable) {
            super(String.format(PREFIX, throwable.getLocalizedMessage()),
                  throwable);
        }
    }

    /**
     * Transform the source value to the target type.
     *
     * @param source - Source value.
     * @return - Transformed value.
     * @throws TransformationException
     */
    T transform(S source) throws TransformationException;

    /**
     * Transform the target value to the source type.
     *
     * @param source - Source value.
     * @return - Transformed value.
     * @throws TransformationException
     */
    S reverse(T source) throws TransformationException;
}
