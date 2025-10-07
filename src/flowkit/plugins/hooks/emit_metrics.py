# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from flowkit.api.hooks import HookAction, HookContext, HookEvent
from flowkit.observability.metrics import SafeCounter, SafeHistogram, get_registry


class EmitMetricsAction(HookAction):  # type: ignore[misc]
    """
    Emit Prometheus metrics with low-cardinality labels.

    Supported args:
      - counter: str   → increment a counter (default: "flowkit_hooks_fired_total")
      - n: int         → increment by N (default: 1)
      - labels: dict   → labels (must be low-cardinality!)
      - histogram: str → observe a value
      - value: float   → value for histogram
      - buckets: list[float] (optional)
      - label_names: list[str] (optional explicit allowlist for labels)

    Notes:
      - If both counter and histogram are provided, both are recorded.
      - Task/node ids are NOT allowed as labels; keep labels stable & small.
    """

    name = "emit.metrics"

    # Simple per-process caches
    _counters: dict[tuple[str, tuple[str, ...]], SafeCounter] = {}
    _hists: dict[tuple[str, tuple[str, ...], tuple[float, ...] | None], SafeHistogram] = {}

    async def run(self, ctx: HookContext, event: HookEvent, args: Mapping[str, Any]) -> None:
        # Defaults
        counter_name = str(args.get("counter") or "flowkit_hooks_fired_total")
        n = int(args.get("n") or 1)
        hist_name = args.get("histogram")
        value = args.get("value")
        buckets = args.get("buckets")

        # Labels: add safe defaults (action, on)
        provided_labels = dict(args.get("labels") or {})
        provided_labels.setdefault("action", self.name)
        provided_labels.setdefault("on", event.on)

        # Validate against allowlist (if provided), else allow given keys as-is.
        label_names = tuple(args.get("label_names") or tuple(provided_labels.keys()))
        labels_tuple = tuple(sorted(label_names))

        # Counter
        if counter_name:
            cnt = self._get_counter(counter_name, labels_tuple)
            cnt.labels(**{k: str(provided_labels.get(k, "")) for k in labels_tuple}).inc(max(0, n))

        # Histogram
        if hist_name and value is not None:
            buckets_tuple = tuple(float(x) for x in (buckets or [])) or None
            hist = self._get_hist(hist_name, labels_tuple, buckets_tuple)
            hist.labels(**{k: str(provided_labels.get(k, "")) for k in labels_tuple}).observe(float(value))

    # ---- caches

    def _get_counter(self, name: str, label_names: tuple[str, ...]) -> SafeCounter:
        key = (name, label_names)
        if key not in self._counters:
            self._counters[key] = SafeCounter(
                name, "FlowKit hook events counter", label_names=label_names, registry=get_registry()
            )
        return self._counters[key]

    def _get_hist(self, name: str, label_names: tuple[str, ...], buckets: tuple[float, ...] | None) -> SafeHistogram:
        key = (name, label_names, buckets)
        if key not in self._hists:
            self._hists[key] = SafeHistogram(
                name,
                "FlowKit hook value histogram",
                label_names=label_names,
                registry=get_registry(),
                buckets=list(buckets) if buckets else None,
            )
        return self._hists[key]
