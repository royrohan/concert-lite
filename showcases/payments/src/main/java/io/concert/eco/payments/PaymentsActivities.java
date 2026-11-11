package io.concert.eco.payments;

import io.concert.common.Env;
import io.concert.eco.payments.external.InMemoryExternals;
import io.concert.eco.payments.external.KafkaNotifyActivities;
import io.concert.eco.payments.external.LedgerHttpActivities;
import io.concert.sdk.WorkerActivities;
import java.util.List;

/**
 * The payment machine's external activities, registered by the generated worker on {@code sm-settlement}
 * (META-INF/services/io.concert.sdk.WorkerActivities): the ledger over HTTP ({@code LEDGER_URL}) and Kafka
 * ({@code PAYMENTS_KAFKA_BOOTSTRAP}, else {@code KAFKA_BOOTSTRAP}). The generated test gets in-memory fakes.
 */
public final class PaymentsActivities implements WorkerActivities {

    @Override
    public List<Object> create(String smType) {
        if (!smType.equals("settlement")) {
            return List.of();
        }
        String ledger = Env.get("LEDGER_URL", "http://localhost:8097");
        String kafka = Env.get("PAYMENTS_KAFKA_BOOTSTRAP", Env.get("KAFKA_BOOTSTRAP", "localhost:29092"));
        return List.of(new LedgerHttpActivities(ledger), new KafkaNotifyActivities(kafka, ledger));
    }

    @Override
    public List<Object> forTests(String smType) {
        return smType.equals("settlement") ? List.of(new InMemoryExternals.Ledger(), new InMemoryExternals.Topic()) : List.of();
    }
}
