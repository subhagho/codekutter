package com.codekutter.common.config;

import com.codekutter.common.config.nodes.ConfigValueNode;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;

public class EncryptedValue extends ConfigValueNode {
    private String path;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public EncryptedValue(@Nonnull ConfigValueNode node) {
        Preconditions.checkArgument(node != null);
        setValue(node.getValue());
        path = node.getSearchPath();
        setConfiguration(node.getConfiguration());
        setParent(node.getParent());

        setEncrypted(true);
    }
}
