package com.codekutter.common.scheduling.remote;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;

@Getter
@Setter
public class JobStateRequest {
    private String nodeId;
    private long requestTime;
    private Set<String> jobCorrelationIds;

    public JobStateRequest addCorrelationId(@Nonnull String correlationId) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(correlationId));
        if (jobCorrelationIds == null) jobCorrelationIds = new HashSet<>();
        jobCorrelationIds.add(correlationId);
        return this;
    }
}
