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

package com.codekutter.r2db.notifications;

/**
 * Exception class used to escalate notification related errors.
 * <p>
 */
public class NotificationException extends Exception {
    /**
     * Error message prefix.
     */
    private static final String __PREFIX = "Notification Error : %s";

    /**
     * Constructor with the specified error message.
     *
     * @param message - Error message.
     */
    public NotificationException(String message) {
        super(String.format(__PREFIX, message));
    }

    /**
     * Constructor with error message and inner exception.
     *
     * @param message - Error message
     * @param cause   - Inner exception.
     */
    public NotificationException(String message, Throwable cause) {
        super(String.format(__PREFIX, message), cause);
    }

    /**
     * Constructor with inner exception.
     *
     * @param cause - Inner exception.
     */
    public NotificationException(Throwable cause) {
        super(String.format(__PREFIX, cause.getLocalizedMessage()), cause);
    }
}
