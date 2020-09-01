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

package com.codekutter.common.scheduling.impl;

import com.codekutter.common.GlobalConstants;
import com.codekutter.common.scheduling.IRestRequestBuilder;
import com.codekutter.common.scheduling.IRestResponseHandler;
import com.codekutter.common.scheduling.JobConfig;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.common.utils.UrlUtils;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigParametersNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.codekutter.zconfig.common.model.nodes.ConfigValueNode;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
public class RestJobConfig extends JobConfig {
    @ConfigValue(required = true)
    private String requestUrl;
    @ConfigAttribute(required = true)
    private ERequestType requestType;
    @ConfigValue(name = "connection", required = true)
    private String connectionName;
    @ConfigValue(required = true)
    private String mediaType;
    @ConfigValue(name = "responseHandler")
    private Class<? extends IRestResponseHandler> responseHandlerClass;
    @ConfigValue(name = "requestBuilder")
    private Class<? extends IRestRequestBuilder> requestBuilderClass;

    @Setter(AccessLevel.NONE)
    @JsonIgnore
    private IRestRequestBuilder requestBuilder;
    @Setter(AccessLevel.NONE)
    @JsonIgnore
    private IRestResponseHandler responseHandler;

    /**
     * Configure this type instance.
     *
     * @param node - Handle to the configuration node.
     * @throws ConfigurationException
     */
    @Override
    public void configure(@Nonnull AbstractConfigNode node) throws ConfigurationException {
        super.configure(node);
        try {
            requestUrl = URLDecoder.decode(requestUrl, GlobalConstants.defaultCharset().name());
            if (node instanceof ConfigPathNode) {
                if (((ConfigPathNode) node).parmeters() != null) {
                    ConfigParametersNode params = ((ConfigPathNode) node).parmeters();
                    if (!params.getKeyValues().isEmpty()) {
                        Map<String, String> values = new HashMap<>();
                        for (String key : params.getKeyValues().keySet()) {
                            ConfigValueNode vn = params.getValue(key);
                            values.put(key, vn.getValue());
                        }
                        if (!values.isEmpty()) {
                            requestUrl = UrlUtils.replaceParams(requestUrl, values);
                        }
                    }
                }
            }
            LogUtils.debug(getClass(), String.format("Request URL [%s]", requestUrl));
            if (responseHandlerClass != null) {
                responseHandler = responseHandlerClass.newInstance();
                responseHandler.configure(node);
            }
            if (requestBuilderClass != null) {
                requestBuilder = requestBuilderClass.newInstance();
            }
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }
}
