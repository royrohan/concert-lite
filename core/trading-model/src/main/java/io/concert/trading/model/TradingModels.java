package io.concert.trading.model;

import io.concert.model.runtime.LegendModel;

/**
 * Generates the Java classes of every trading Pure model at compile time: {@code trading::<pkg>::X}
 * becomes {@code io.concert.trading.<pkg>.X}, e.g. {@code io.concert.trading.command.FillCommand},
 * {@code io.concert.trading.order.Order}, {@code io.concert.trading.refdata.Instrument},
 * {@code io.concert.trading.marketdata.Tick}. Each file's registry class ({@code <Stem>Model}: Mermaid
 * diagram and class list) goes into {@code io.concert.trading}, e.g. {@code io.concert.trading.OrderModel}.
 *
 * <p>The files form one file set, so every class is generated exactly once, in this module. Phase B's
 * state machines name the same {@code files} and {@code javaPackage} (with their own {@code root}); the
 * processor generates a file into one package only.
 */
@LegendModel(
        files = {"common.pure", "refdata.pure", "marketdata.pure", "order.pure", "commands.pure"},
        root = "trading::order::Order",
        javaPackage = "io.concert")
public final class TradingModels {
    private TradingModels() {}

    /** The {@code files} of the annotation, for code that resolves the models itself (DDL generators, tests). */
    public static final String[] FILES = {"common.pure", "refdata.pure", "marketdata.pure", "order.pure", "commands.pure"};
}
