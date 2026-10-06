package io.concert.common;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;

/** Builds a Temporal client from TEMPORAL_ADDRESS / TEMPORAL_NAMESPACE. */
public final class TemporalClients {
    private TemporalClients() {}

    public static WorkflowClient fromEnv() {
        return create(Env.get("TEMPORAL_ADDRESS", "localhost:7233"), Env.get("TEMPORAL_NAMESPACE", "default"));
    }

    public static WorkflowClient create(String address, String namespace) {
        WorkflowServiceStubs service = WorkflowServiceStubs.newServiceStubs(
                WorkflowServiceStubsOptions.newBuilder().setTarget(address).build());
        return WorkflowClient.newInstance(
                service, WorkflowClientOptions.newBuilder().setNamespace(namespace).build());
    }
}
