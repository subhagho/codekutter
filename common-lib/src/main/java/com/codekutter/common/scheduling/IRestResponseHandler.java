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

package com.codekutter.common.scheduling;

import com.codekutter.zconfig.common.IConfigurable;
import org.quartz.JobExecutionException;

import javax.annotation.Nonnull;
import javax.ws.rs.core.Response;

/**
 * Interface to define REST response handlers for scheduled Jobs.
 */
public interface IRestResponseHandler extends IConfigurable {
    /**
     * Handle response.
     *
     * @param response - REST call response.
     * @return - Processed response object.
     * @throws JobExecutionException
     */
    @Nonnull Object handle(@Nonnull Response response) throws JobExecutionException;
}
