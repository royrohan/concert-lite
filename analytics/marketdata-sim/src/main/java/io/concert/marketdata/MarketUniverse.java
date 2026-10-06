package io.concert.marketdata;

import io.concert.trading.common.AssetClass;
import io.concert.trading.common.Currency;
import io.concert.trading.common.Sector;
import io.concert.trading.refdata.Account;
import io.concert.trading.refdata.AccountType;
import io.concert.trading.refdata.Instrument;
import io.concert.trading.refdata.Venue;
import io.concert.trading.refdata.VenueType;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SplittableRandom;

/**
 * The simulated market: instruments, accounts and venues, deterministic from a seed. This is the
 * single source of the symbol universe; the trading generator selects from the same lists, so its
 * orders reference symbols, accounts and venues that exist in {@code ref.*}.
 *
 * <p>All types are generated from {@code trading-model}'s {@code refdata.pure}; {@code updatedAt} is
 * left unset here and stamped by the publisher.
 *
 * <p>Symbols and names are a fixed list of US large caps (plus two ETFs) for readability; the seed
 * perturbs reference prices (±15%) and generates ISINs and accounts.
 */
public record MarketUniverse(List<Instrument> instruments, List<Account> accounts, List<Venue> venues) {

    public static final int DEFAULT_ACCOUNTS = 20;

    public MarketUniverse {
        instruments = List.copyOf(instruments);
        accounts = List.copyOf(accounts);
        venues = List.copyOf(venues);
    }

    private record Listing(String symbol, String name, Sector sector, double price, String mic) {}

    private static final List<Listing> LISTINGS = List.of(
            new Listing("AAPL", "Apple Inc.", Sector.TECHNOLOGY, 230, "XNAS"),
            new Listing("MSFT", "Microsoft Corp.", Sector.TECHNOLOGY, 430, "XNAS"),
            new Listing("NVDA", "NVIDIA Corp.", Sector.TECHNOLOGY, 125, "XNAS"),
            new Listing("AVGO", "Broadcom Inc.", Sector.TECHNOLOGY, 170, "XNAS"),
            new Listing("ORCL", "Oracle Corp.", Sector.TECHNOLOGY, 170, "XNYS"),
            new Listing("CRM", "Salesforce Inc.", Sector.TECHNOLOGY, 280, "XNYS"),
            new Listing("ADBE", "Adobe Inc.", Sector.TECHNOLOGY, 500, "XNAS"),
            new Listing("AMD", "Advanced Micro Devices Inc.", Sector.TECHNOLOGY, 150, "XNAS"),
            new Listing("GOOGL", "Alphabet Inc. Class A", Sector.COMMUNICATION, 165, "XNAS"),
            new Listing("META", "Meta Platforms Inc.", Sector.COMMUNICATION, 580, "XNAS"),
            new Listing("NFLX", "Netflix Inc.", Sector.COMMUNICATION, 700, "XNAS"),
            new Listing("DIS", "Walt Disney Co.", Sector.COMMUNICATION, 95, "XNYS"),
            new Listing("VZ", "Verizon Communications Inc.", Sector.COMMUNICATION, 43, "XNYS"),
            new Listing("AMZN", "Amazon.com Inc.", Sector.CONSUMER_DISCRETIONARY, 185, "XNAS"),
            new Listing("TSLA", "Tesla Inc.", Sector.CONSUMER_DISCRETIONARY, 250, "XNAS"),
            new Listing("HD", "Home Depot Inc.", Sector.CONSUMER_DISCRETIONARY, 400, "XNYS"),
            new Listing("MCD", "McDonald's Corp.", Sector.CONSUMER_DISCRETIONARY, 300, "XNYS"),
            new Listing("NKE", "Nike Inc.", Sector.CONSUMER_DISCRETIONARY, 85, "XNYS"),
            new Listing("WMT", "Walmart Inc.", Sector.CONSUMER_STAPLES, 80, "XNYS"),
            new Listing("PG", "Procter & Gamble Co.", Sector.CONSUMER_STAPLES, 170, "XNYS"),
            new Listing("KO", "Coca-Cola Co.", Sector.CONSUMER_STAPLES, 70, "XNYS"),
            new Listing("COST", "Costco Wholesale Corp.", Sector.CONSUMER_STAPLES, 890, "XNAS"),
            new Listing("JPM", "JPMorgan Chase & Co.", Sector.FINANCIALS, 215, "XNYS"),
            new Listing("BAC", "Bank of America Corp.", Sector.FINANCIALS, 40, "XNYS"),
            new Listing("GS", "Goldman Sachs Group Inc.", Sector.FINANCIALS, 500, "XNYS"),
            new Listing("MS", "Morgan Stanley", Sector.FINANCIALS, 105, "XNYS"),
            new Listing("V", "Visa Inc.", Sector.FINANCIALS, 280, "XNYS"),
            new Listing("MA", "Mastercard Inc.", Sector.FINANCIALS, 500, "XNYS"),
            new Listing("BLK", "BlackRock Inc.", Sector.FINANCIALS, 950, "XNYS"),
            new Listing("JNJ", "Johnson & Johnson", Sector.HEALTHCARE, 160, "XNYS"),
            new Listing("UNH", "UnitedHealth Group Inc.", Sector.HEALTHCARE, 580, "XNYS"),
            new Listing("LLY", "Eli Lilly & Co.", Sector.HEALTHCARE, 900, "XNYS"),
            new Listing("PFE", "Pfizer Inc.", Sector.HEALTHCARE, 29, "XNYS"),
            new Listing("MRK", "Merck & Co. Inc.", Sector.HEALTHCARE, 110, "XNYS"),
            new Listing("ABBV", "AbbVie Inc.", Sector.HEALTHCARE, 195, "XNYS"),
            new Listing("XOM", "Exxon Mobil Corp.", Sector.ENERGY, 118, "XNYS"),
            new Listing("CVX", "Chevron Corp.", Sector.ENERGY, 150, "XNYS"),
            new Listing("COP", "ConocoPhillips", Sector.ENERGY, 108, "XNYS"),
            new Listing("CAT", "Caterpillar Inc.", Sector.INDUSTRIALS, 380, "XNYS"),
            new Listing("BA", "Boeing Co.", Sector.INDUSTRIALS, 155, "XNYS"),
            new Listing("HON", "Honeywell International Inc.", Sector.INDUSTRIALS, 210, "XNAS"),
            new Listing("GE", "GE Aerospace", Sector.INDUSTRIALS, 185, "XNYS"),
            new Listing("NEE", "NextEra Energy Inc.", Sector.UTILITIES, 82, "XNYS"),
            new Listing("DUK", "Duke Energy Corp.", Sector.UTILITIES, 115, "XNYS"),
            new Listing("SO", "Southern Co.", Sector.UTILITIES, 90, "XNYS"),
            new Listing("LIN", "Linde plc", Sector.MATERIALS, 470, "XNAS"),
            new Listing("FCX", "Freeport-McMoRan Inc.", Sector.MATERIALS, 48, "XNYS"),
            new Listing("NEM", "Newmont Corp.", Sector.MATERIALS, 53, "XNYS"),
            new Listing("PLD", "Prologis Inc.", Sector.REAL_ESTATE, 125, "XNYS"),
            new Listing("AMT", "American Tower Corp.", Sector.REAL_ESTATE, 230, "XNYS"),
            new Listing("SPY", "SPDR S&P 500 ETF Trust", Sector.BROAD_MARKET, 570, "ARCX"),
            new Listing("QQQ", "Invesco QQQ Trust", Sector.BROAD_MARKET, 490, "XNAS"));

    private static final List<Venue> VENUES = List.of(
            venue("XNAS", "Nasdaq Stock Market", VenueType.LIT, "-0.0020", "0.0030"),
            venue("XNYS", "New York Stock Exchange", VenueType.LIT, "-0.0018", "0.0030"),
            venue("ARCX", "NYSE Arca", VenueType.LIT, "-0.0020", "0.0030"),
            venue("BATS", "Cboe BZX Exchange", VenueType.LIT, "-0.0021", "0.0030"),
            venue("EDGX", "Cboe EDGX Exchange", VenueType.LIT, "-0.0019", "0.0029"),
            venue("IEXG", "Investors Exchange", VenueType.LIT, "0.0009", "0.0009"),
            venue("MEMX", "Members Exchange", VenueType.LIT, "-0.0020", "0.0028"),
            // Not a real MIC: the simulated broker's own dark pool.
            venue("XSIM", "Simulated Dark Pool", VenueType.DARK, "0.0010", "0.0010"));

    private static final String[] FUND_PREFIX = {"Northwind", "Blue Heron", "Granite Peak", "Silver Birch", "Harbor Light",
            "Red Kite", "Ironwood", "Clearwater", "Summit Ridge", "Old Mill", "Tidewater", "Copper Fox"};
    private static final String[] FUND_SUFFIX = {"Capital", "Partners", "Asset Management", "Advisors", "Investments"};
    private static final String[] DESKS = {"US Equities", "Program Trading", "Event Driven", "Quant", "Retail Flow"};

    private static Venue venue(String mic, String name, VenueType type, String maker, String taker) {
        return new Venue().setMic(mic).setName(name).setType(type).setMakerFeePerShare(new BigDecimal(maker))
                .setTakerFeePerShare(new BigDecimal(taker));
    }

    public static MarketUniverse create(long seed) {
        return create(seed, DEFAULT_ACCOUNTS);
    }

    public static MarketUniverse create(long seed, int accountCount) {
        SplittableRandom rnd = new SplittableRandom(seed);
        BigDecimal cent = new BigDecimal("0.01");
        List<Instrument> instruments = new ArrayList<>(LISTINGS.size());
        for (Listing l : LISTINGS) {
            double px = l.price() * (0.85 + 0.30 * rnd.nextDouble());
            instruments.add(new Instrument().setSymbol(l.symbol()).setIsin(isin(rnd)).setName(l.name())
                    .setAssetClass(l.sector() == Sector.BROAD_MARKET ? AssetClass.ETF : AssetClass.EQUITY).setSector(l.sector())
                    .setCurrency(Currency.USD).setLotSize(100L).setTickSize(cent)
                    .setRefPrice(BigDecimal.valueOf(px).setScale(2, RoundingMode.HALF_EVEN)).setPrimaryMic(l.mic()));
        }
        List<Account> accounts = new ArrayList<>(accountCount);
        AccountType[] types = AccountType.values();
        for (int i = 1; i <= accountCount; i++) {
            String name = FUND_PREFIX[rnd.nextInt(FUND_PREFIX.length)] + " " + FUND_SUFFIX[rnd.nextInt(FUND_SUFFIX.length)];
            long limitMillions = 5 + rnd.nextInt(46);
            accounts.add(new Account().setAccountId("ACC-%03d".formatted(i)).setName(name).setDesk(DESKS[rnd.nextInt(DESKS.length)])
                    .setType(types[rnd.nextInt(types.length)]).setBaseCurrency(Currency.USD)
                    .setCreditLimit(BigDecimal.valueOf(limitMillions * 1_000_000L)));
        }
        return new MarketUniverse(instruments, accounts, VENUES);
    }

    /** All symbols, in universe order. */
    public List<String> symbols() {
        return instruments.stream().map(Instrument::getSymbol).toList();
    }

    public Instrument instrument(String symbol) {
        return instruments.stream().filter(i -> i.getSymbol().equals(symbol)).findFirst()
                .orElseThrow(() -> new NoSuchElementException("unknown symbol " + symbol));
    }

    public Venue venue(String mic) {
        return venues.stream().filter(v -> v.getMic().equals(mic)).findFirst()
                .orElseThrow(() -> new NoSuchElementException("unknown venue " + mic));
    }

    /**
     * Instruments selected by a CLI spec: blank or {@code all} = every symbol, a number {@code N} = the
     * first N, otherwise a comma-separated symbol list.
     */
    public List<Instrument> selectInstruments(String spec) {
        return select(spec, instruments, Instrument::getSymbol);
    }

    /** Accounts selected by a CLI spec, with the same rules as {@link #selectInstruments}. */
    public List<Account> selectAccounts(String spec) {
        return select(spec, accounts, Account::getAccountId);
    }

    private static <T> List<T> select(String spec, List<T> all, java.util.function.Function<T, String> id) {
        if (spec == null || spec.isBlank() || spec.equalsIgnoreCase("all")) {
            return all;
        }
        if (spec.chars().allMatch(Character::isDigit)) {
            int n = Integer.parseInt(spec);
            if (n < 1 || n > all.size()) {
                throw new IllegalArgumentException("need 1.." + all.size() + ", got " + n);
            }
            return all.subList(0, n);
        }
        Map<String, T> byId = new LinkedHashMap<>();
        all.forEach(t -> byId.put(id.apply(t), t));
        return Arrays.stream(spec.split(",")).map(String::trim).filter(s -> !s.isEmpty()).map(s -> {
            T t = byId.get(s);
            if (t == null) {
                throw new IllegalArgumentException("unknown id " + s);
            }
            return t;
        }).toList();
    }

    /** A synthetic US ISIN: "US" + 9 random alphanumerics + the ISO 6166 (Luhn) check digit. */
    static String isin(SplittableRandom rnd) {
        String alphabet = "0123456789ABCDEFGHJKLMNPQRSTUVWXYZ";
        StringBuilder body = new StringBuilder("US");
        for (int i = 0; i < 9; i++) {
            body.append(alphabet.charAt(rnd.nextInt(alphabet.length())));
        }
        return body.toString() + isinCheckDigit(body.toString());
    }

    static int isinCheckDigit(String first11) {
        StringBuilder digits = new StringBuilder();
        for (char c : first11.toCharArray()) {
            digits.append(Character.isDigit(c) ? String.valueOf(c - '0') : String.valueOf(c - 'A' + 10));
        }
        int sum = 0;
        boolean dbl = true; // rightmost digit of the payload is doubled
        for (int i = digits.length() - 1; i >= 0; i--) {
            int d = digits.charAt(i) - '0';
            if (dbl) {
                d *= 2;
                if (d > 9) {
                    d -= 9;
                }
            }
            sum += d;
            dbl = !dbl;
        }
        return (10 - sum % 10) % 10;
    }
}
