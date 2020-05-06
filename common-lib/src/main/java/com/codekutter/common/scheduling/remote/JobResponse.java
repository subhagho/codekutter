package com.codekutter.common.scheduling.remote;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JobResponse {
    private String requestId;
    private String correlationId;
    private JobState jobState;
    private long requestTime;
    private long completedTime;
}
