package com.codekutter.common.scheduling.remote;

import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
public class JobStateResponse {
    private JobStateRequest request;
    private long responseTime;
    private Map<String, JobResponse> states;

    public JobStateResponse() {
    }

    public JobStateResponse(@Nonnull JobStateRequest request) {
        this.request = request;
    }

    public JobStateResponse addJobState(@Nonnull JobResponse pJobResponse) {
        if (request.getJobCorrelationIds() != null
                && request.getJobCorrelationIds().contains(pJobResponse.getCorrelationId())) {
            if (states == null) states = new HashMap<>();
            states.put(pJobResponse.getCorrelationId(), pJobResponse);
        }
        return this;
    }

    public JobResponse findResponse(@Nonnull String correlationId) {
        if (states != null) {
            return states.get(correlationId);
        }
        return null;
    }
}
