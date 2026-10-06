package io.concert.samples;

import io.concert.common.EventEnvelope;
import io.concert.model.runtime.LegendModel;
import io.concert.samples.model.demo.shipment.DeliverCommand;
import io.concert.samples.model.demo.shipment.DispatchCommand;
import io.concert.samples.model.demo.shipment.LoseCommand;
import io.concert.samples.model.demo.shipment.Parcel;
import io.concert.samples.model.demo.shipment.PickCommand;
import io.concert.samples.model.demo.shipment.Shipment;
import io.concert.samples.model.demo.shipment.ShipmentStatus;
import io.concert.sdk.StateMachineSpec;
import java.util.HashSet;
import java.util.Set;

/** Typed shipment machine: entity data is {@link Shipment} from {@code shipment.pure}, payloads are its commands. */
@LegendModel(files = {"common.pure", "shipment.pure"}, root = "demo::shipment::Shipment")
public class ShipmentStateMachine extends SampleModelStateMachine<Shipment> {

    public static final String TYPE = "shipment";

    public static final StateMachineSpec SPEC = StateMachineSpec.startingAt("CREATED")
            .on("CREATED", "pick", "PICKED", PickCommand.class)
            .on("PICKED", "dispatch", "IN_TRANSIT", DispatchCommand.class)
            .on("IN_TRANSIT", "deliver", "DELIVERED", DeliverCommand.class)
            .on("IN_TRANSIT", "lose", "LOST", LoseCommand.class)
            .build();

    @Override
    protected StateMachineSpec spec() {
        return SPEC;
    }

    @Override
    protected Class<Shipment> dataType() {
        return Shipment.class;
    }

    @Override
    protected Shipment initialData(String instanceKey) {
        return new Shipment().setShipmentId(instanceKey);
    }

    /**
     * Defaults for a missing command or missing required fields: {@code dispatch} uses carrier
     * {@code UNASSIGNED} and tracking number {@code TRK-<shipmentId>}; {@code lose} the reason
     * {@code unknown}.
     */
    @Override
    protected Object completePayload(String fromState, String eventType, Object payload, Shipment shipment) {
        return switch (eventType) {
            case "pick" -> payload != null ? payload : new PickCommand();
            case "dispatch" -> {
                DispatchCommand dispatch = payload != null ? (DispatchCommand) payload : new DispatchCommand();
                if (dispatch.getCarrier() == null) {
                    dispatch.setCarrier("UNASSIGNED");
                }
                yield dispatch.getTrackingNumber() != null ? dispatch : dispatch.setTrackingNumber("TRK-" + shipment.getShipmentId());
            }
            case "deliver" -> payload != null ? payload : new DeliverCommand();
            case "lose" -> {
                LoseCommand lose = payload != null ? (LoseCommand) payload : new LoseCommand();
                yield lose.getReason() != null ? lose : lose.setReason("unknown");
            }
            default -> payload;
        };
    }

    @Override
    protected void onTransition(String from, String to, Shipment shipment, Object payload, EventEnvelope event) {
        shipment.setStatus(ShipmentStatus.valueOf(to));
        switch (payload) {
            case PickCommand pick -> {
                Set<String> ids = new HashSet<>();
                for (Parcel parcel : pick.getParcels()) {
                    if (!ids.add(parcel.getParcelId())) {
                        reject("duplicate parcel " + parcel.getParcelId());
                    }
                    shipment.addParcel(parcel);
                }
                shipment.setOrderId(pick.getOrderId())
                        .setDestination(pick.getDestination())
                        .setInsuredValue(pick.getInsuredValue())
                        .setPickedAt(now());
                simulateWork(pick.getWorkMs());
            }
            case DispatchCommand dispatch -> {
                shipment.setCarrier(dispatch.getCarrier()).setTrackingNumber(dispatch.getTrackingNumber()).setDispatchedAt(now());
                simulateWork(dispatch.getWorkMs());
            }
            case DeliverCommand deliver -> {
                shipment.setSignedBy(deliver.getSignedBy()).setDeliveredAt(now());
                simulateWork(deliver.getWorkMs());
            }
            case LoseCommand lose -> {
                shipment.setLossReason(lose.getReason()).setLostAt(now());
                simulateWork(lose.getWorkMs());
            }
            case null, default -> throw new IllegalStateException("unexpected payload for " + event.eventType() + ": " + payload);
        }
    }
}
