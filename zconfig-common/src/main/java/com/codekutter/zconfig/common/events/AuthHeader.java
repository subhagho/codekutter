package com.codekutter.zconfig.common.events;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public abstract class AuthHeader {
    /**
     * Authentication type this auth header uses.
     */
    private EAuthType authType;
}
