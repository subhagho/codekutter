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

package com.codekutter.common;

import com.codekutter.common.scheduling.IRestResponseHandler;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import org.quartz.JobExecutionException;

import javax.annotation.Nonnull;
import javax.ws.rs.core.Response;

public class DummyResponseHandler implements IRestResponseHandler {
    /**
     * Handle response.
     *
     * @param response - REST call response.
     * @throws JobExecutionException
     */
    @Override
    public @Nonnull Object handle(@Nonnull Response response) throws JobExecutionException {
        String output = response.readEntity(String.class);
        LogUtils.debug(getClass(), output);

        return output;
    }

    /**
     * Configure this type instance.
     *
     * @param node - Handle to the configuration node.
     * @throws ConfigurationException
     */
    @Override
    public void configure(@Nonnull AbstractConfigNode node) throws ConfigurationException {

    }
}
