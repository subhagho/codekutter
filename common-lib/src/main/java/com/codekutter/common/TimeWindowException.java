/*
 *
 *  * Copyright 2014 Subhabrata Ghosh
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.codekutter.common;

/**
 * Exception type escalated for Time window related errors.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 04/08/14
 */
@SuppressWarnings("serial")
public class TimeWindowException extends Exception {
    private static final String _PREFIX_ = "Time Window Exception : ";

    /**
     * Instantiates a new time window exception with the exception message
     *
     * @param mesg
     *            the exception mesg
     */
    public TimeWindowException(String mesg) {
        super(_PREFIX_ + mesg);
    }

    /**
     * Instantiates a new time window exception with the exception message and
     * exception cause
     *
     * @param mesg
     *            the exception mesg
     * @param inner
     *            the throwable cause
     */
    public TimeWindowException(String mesg, Throwable inner) {
        super(_PREFIX_ + mesg, inner);
    }
}
