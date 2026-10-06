package io.concert.samples;

import io.concert.samples.model.demo.common.Currency;
import io.concert.samples.model.demo.common.Money;
import java.math.BigDecimal;
import java.util.List;
import java.util.function.Function;

/** Arithmetic on the generated {@link Money} class (amounts compare by value, not scale). */
final class Moneys {
    private Moneys() {}

    static Money of(String amount, Currency currency) {
        return new Money().setAmount(new BigDecimal(amount)).setCurrency(currency);
    }

    static Money zero() {
        return of("0", Currency.USD);
    }

    static Money copy(Money m) {
        return m == null ? null : new Money().setAmount(m.getAmount()).setCurrency(m.getCurrency());
    }

    static boolean sameCurrency(Money a, Money b) {
        return a.getCurrency() == b.getCurrency();
    }

    /** Same currency and numerically equal amounts. */
    static boolean equal(Money a, Money b) {
        return sameCurrency(a, b) && a.getAmount().compareTo(b.getAmount()) == 0;
    }

    /** {@code a <= b}, in the same currency. */
    static boolean atMost(Money a, Money b) {
        return sameCurrency(a, b) && a.getAmount().compareTo(b.getAmount()) <= 0;
    }

    static Money times(Money m, long quantity) {
        return new Money().setAmount(m.getAmount().multiply(BigDecimal.valueOf(quantity))).setCurrency(m.getCurrency());
    }

    /** Sum of {@code items} mapped to money; {@code null} if empty or the currencies differ. */
    static <T> Money sum(List<T> items, Function<T, Money> money) {
        Money total = null;
        for (T item : items) {
            Money m = money.apply(item);
            if (total == null) {
                total = copy(m);
            } else if (!sameCurrency(total, m)) {
                return null;
            } else {
                total.setAmount(total.getAmount().add(m.getAmount()));
            }
        }
        return total;
    }

    static String format(Money m) {
        return m.getAmount().toPlainString() + " " + m.getCurrency();
    }
}
