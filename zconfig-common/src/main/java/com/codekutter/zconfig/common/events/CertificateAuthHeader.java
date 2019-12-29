package com.codekutter.zconfig.common.events;

import lombok.Getter;
import lombok.Setter;

/**
 *
 */
@Getter
@Setter
public class CertificateAuthHeader extends AuthHeader {
    private String certificate;

    public CertificateAuthHeader() {
        setAuthType(EAuthType.Certificate);
    }
}
