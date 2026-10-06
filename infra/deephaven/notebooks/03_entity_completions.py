# Completed concert entities.
# snapshots = latest version per entity from the entity-snapshots topic (every entity that reached a
# terminal state); the concert app also builds completion_latency, state_distribution,
# completions_per_minute and per-type tables (orders_latest, payments_latest, shipments_latest).
from deephaven import agg
from deephaven.plot.figure import Figure

# Latency percentiles per type and terminal state.
latency_by_state = snapshots.where("!isNull(LatencyMs)").agg_by([
    agg.count_("Entities"),
    agg.pct(0.50, ["P50Ms = LatencyMs"]),
    agg.pct(0.95, ["P95Ms = LatencyMs"]),
    agg.pct(0.99, ["P99Ms = LatencyMs"]),
    agg.max_(["MaxMs = LatencyMs"]),
], by=["SmType", "State"])

# The 20 slowest entities, with their model as JSON text.
slowest = snapshots.where("!isNull(LatencyMs)").sort_descending("LatencyMs").head(20) \
    .view(["EntityId", "SmType", "State", "LatencyMs", "CompletedAt", "ModelJson"])

# Terminal state shares per type.
state_share = state_distribution.update_view("Key = SmType + `/` + State")
state_plot = Figure().plot_cat(series_name="entities", t=state_share, category="Key", y="Entities").show()

# Completions per minute as a ticking line per type.
per_minute_plot = (Figure()
                   .plot_xy(series_name="completions", t=completions_per_minute.sort("Minute"), x="Minute",
                            y="Completions", by=["SmType"])
                   .show())
