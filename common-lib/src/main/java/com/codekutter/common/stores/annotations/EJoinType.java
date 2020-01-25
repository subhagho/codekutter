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

package com.codekutter.common.stores.annotations;

/**
 * Enum to specify join type for Reference
 * definitions.
 */
public enum EJoinType {
    /**
     * One to One Join
     */
    One2One,
    /**
     * One to Many Join
     */
    One2Many,
    /**
     * Many to One Join
     */
    Many2One,
    /**
     * Many to Many join.
     */
    Many2Many
}
