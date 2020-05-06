package com.codekutter.zconfig.common.transformers;

import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.annotations.ICustomParser;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigValueNode;

import javax.annotation.Nonnull;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public class URLEncodedParser implements ICustomParser<String> {
    /**
     * Parse the input configuration node to generate the return value.
     *
     * @param node - Configuration node.
     * @param name - Value name.
     * @return - Parsed value.
     * @throws ConfigurationException
     */
    @Override
    public String parse(@Nonnull AbstractConfigNode node, @Nonnull String name) throws ConfigurationException {
        node = node.find(name);
        try {
            if (node instanceof ConfigValueNode) {
                ConfigValueNode vn = (ConfigValueNode) node;
                String value = vn.getValue();
                return URLDecoder.decode(value, StandardCharsets.UTF_8.name());
            }
            return null;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }
}
