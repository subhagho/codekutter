package com.codekutter.zconfig.common.events;

import lombok.Getter;
import lombok.Setter;

/**
 * User/Password based authentication.
 */
@Getter
@Setter
public class UserBasedAuthHeader extends AuthHeader {
    private String username;
    private String password;

    /**
     * Default empty constructor - set the auth type to user based.
     */
    public UserBasedAuthHeader() {
        setAuthType(EAuthType.UserBased);
    }
}
