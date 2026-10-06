package io.concert.samples;

import io.concert.common.EventEnvelope;
import io.concert.model.runtime.LegendModel;
import io.concert.samples.model.demo.common.Money;
import io.concert.samples.model.demo.common.PaymentMethod;
import io.concert.samples.model.demo.payment.AuthorizeCommand;
import io.concert.samples.model.demo.payment.CaptureCommand;
import io.concert.samples.model.demo.payment.DeclineCommand;
import io.concert.samples.model.demo.payment.EntryKind;
import io.concert.samples.model.demo.payment.Payment;
import io.concert.samples.model.demo.payment.PaymentEntry;
import io.concert.samples.model.demo.payment.PaymentStatus;
import io.concert.samples.model.demo.payment.RefundCommand;
import io.concert.samples.model.demo.payment.VoidCommand;
import io.concert.sdk.StateMachineSpec;

/** Typed payment machine: entity data is {@link Payment} from {@code payment.pure}, payloads are its commands. */
@LegendModel(files = {"common.pure", "payment.pure"}, root = "demo::payment::Payment")
public class PaymentStateMachine extends SampleModelStateMachine<Payment> {

    public static final String TYPE = "payment";

    public static final StateMachineSpec SPEC = StateMachineSpec.startingAt("PENDING")
            .on("PENDING", "authorize", "AUTHORIZED", AuthorizeCommand.class)
            .on("PENDING", "decline", "DECLINED", DeclineCommand.class)
            .on("AUTHORIZED", "capture", "CAPTURED", CaptureCommand.class)
            .on("AUTHORIZED", "void", "VOIDED", VoidCommand.class)
            .on("CAPTURED", "refund", "REFUNDED", RefundCommand.class)
            .build();

    @Override
    protected StateMachineSpec spec() {
        return SPEC;
    }

    @Override
    protected Class<Payment> dataType() {
        return Payment.class;
    }

    @Override
    protected Payment initialData(String instanceKey) {
        return new Payment().setPaymentId(instanceKey);
    }

    /**
     * Defaults for a missing command or missing required fields: {@code authorize} 0 USD by
     * {@code CARD}; {@code decline} the reason {@code declined}. Capture and refund amounts default
     * to the full authorized / captured amount in {@link #onTransition}.
     */
    @Override
    protected Object completePayload(String fromState, String eventType, Object payload, Payment payment) {
        return switch (eventType) {
            case "authorize" -> {
                AuthorizeCommand auth = payload != null ? (AuthorizeCommand) payload : new AuthorizeCommand();
                if (auth.getAmount() == null) {
                    auth.setAmount(Moneys.zero());
                }
                yield auth.getMethod() != null ? auth : auth.setMethod(PaymentMethod.CARD);
            }
            case "capture" -> payload != null ? payload : new CaptureCommand();
            case "refund" -> payload != null ? payload : new RefundCommand();
            case "decline" -> {
                DeclineCommand decline = payload != null ? (DeclineCommand) payload : new DeclineCommand();
                yield decline.getReason() != null ? decline : decline.setReason("declined");
            }
            case "void" -> payload != null ? payload : new VoidCommand();
            default -> payload;
        };
    }

    @Override
    protected void onTransition(String from, String to, Payment payment, Object payload, EventEnvelope event) {
        payment.setStatus(PaymentStatus.valueOf(to));
        switch (payload) {
            case AuthorizeCommand auth -> {
                payment.setAuthorized(Moneys.copy(auth.getAmount()))
                        .setMethod(auth.getMethod())
                        .setOrderId(auth.getOrderId())
                        .setAuthorizationCode("AUTH-" + event.eventId());
                entry(payment, EntryKind.AUTHORIZATION, auth.getAmount(), event);
                simulateWork(auth.getWorkMs());
            }
            case CaptureCommand capture -> {
                Money amount = capture.getAmount() != null ? capture.getAmount() : payment.getAuthorized();
                if (!Moneys.atMost(amount, payment.getAuthorized())) {
                    reject("capture of " + Moneys.format(amount) + " exceeds authorized " + Moneys.format(payment.getAuthorized()));
                }
                payment.setCaptured(Moneys.copy(amount));
                entry(payment, EntryKind.CAPTURE, amount, event);
                simulateWork(capture.getWorkMs());
            }
            case RefundCommand refund -> {
                Money amount = refund.getAmount() != null ? refund.getAmount() : payment.getCaptured();
                if (!Moneys.atMost(amount, payment.getCaptured())) {
                    reject("refund of " + Moneys.format(amount) + " exceeds captured " + Moneys.format(payment.getCaptured()));
                }
                payment.setRefunded(Moneys.copy(amount)).setClosedAt(now());
                entry(payment, EntryKind.REFUND, amount, event);
                simulateWork(refund.getWorkMs());
            }
            case DeclineCommand decline -> {
                payment.setDeclineReason(decline.getReason()).setClosedAt(now());
                simulateWork(decline.getWorkMs());
            }
            case VoidCommand voided -> {
                payment.setClosedAt(now());
                entry(payment, EntryKind.VOID, payment.getAuthorized(), event);
                simulateWork(voided.getWorkMs());
            }
            case null, default -> throw new IllegalStateException("unexpected payload for " + event.eventType() + ": " + payload);
        }
    }

    private static void entry(Payment payment, EntryKind kind, Money amount, EventEnvelope event) {
        payment.addEntry(new PaymentEntry().setKind(kind).setAmount(Moneys.copy(amount)).setAt(now()).setReference(event.eventId()));
    }
}
