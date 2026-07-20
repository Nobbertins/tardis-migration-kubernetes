#!/usr/bin/env python3
"""
tardis_controller.py
────────────────────
TARDIS-augmented pod migration controller.

Extends the peak-load controller with temporal relationship embeddings
(TARDIS, SIGCOMM'26) to improve migration decisions:
  - Victim selection: among pods with comparable load, prefer the one
    with the most temporally synchronized peers on the same node
    (highest RelatedPeak), as migrating it removes the most future
    burst overlap.
  - Destination feasibility: relaxes the peak-load check by considering
    whether the victim's embedding cluster on the destination already
    has non-overlapping peaks, per Algorithm 2 of the paper.
  - Target selection: among feasible destinations, prefer the one whose
    embedding cluster has the least peak overlap with the victim.

Migration mechanics (create -> wait -> drain -> delete) and kubectl/
scraping helpers live in migration_common.py, shared with
peakload_controller.py and avgload_controller.py. This file adds two
hooks to that shared sequence: transfer the TARDIS embedding to the new
pod name right before draining, and forget the old pod's embedding right
after it's deleted.

Environment variables:
  POLL_INTERVAL        seconds between migration decisions (default 5)
  SCRAPE_INTERVAL      seconds between collector scrapes (default 1)
  LOAD_THRESHOLD       node cpu % that triggers hotspot detection (default 80)
  NAMESPACE            pod namespace to watch — see migration_common.py
  COLLECTOR_PORT       collector daemonset port — see migration_common.py
  DRY_RUN              if "true", print decisions without evicting — see migration_common.py

  # TARDIS hyperparameters — fill in from estimate_tardis_params.py output
  TARDIS_ALPHA         embedding learning rate (default 0.1)
  TARDIS_BETA          context vector drift speed (default 0.05)
  TARDIS_TAU           cosine similarity threshold for "related" (default 0.5)
  TARDIS_ACTIVITY_PCT  per-pod CPU% floor to count as active (default 20.0)
  TARDIS_D             embedding dimension per process (default 10)
  TARDIS_K             number of parallel ensemble processes (default 8)
  TARDIS_LOAD_EPS      load bin width for victim/dest grouping (default 5.0)
"""

import asyncio
import math
import os
import time
from collections import defaultdict

import aiohttp
import numpy as np

from migration_common import (
    PeakTracker,
    get_collector_pod_ips,
    get_worker_pods,
    scrape_all,
    run_migration,
)

# ── config ────────────────────────────────────────────────────────────────────
POLL_INTERVAL   = 15.0
SCRAPE_INTERVAL = 3.0
LOAD_THRESHOLD  = float(os.environ.get("LOAD_THRESHOLD", "80"))

# TARDIS hyperparameters — set these from estimate_tardis_params.py output
TARDIS_ALPHA        = float(os.environ.get("TARDIS_ALPHA",        "0.4472"))
TARDIS_BETA         = float(os.environ.get("TARDIS_BETA",         "0.5"))
TARDIS_TAU          = float(os.environ.get("TARDIS_TAU",          "0.157"))
TARDIS_ACTIVITY_PCT = float(os.environ.get("TARDIS_ACTIVITY_PCT", "10.0"))
TARDIS_D            = int(os.environ.get("TARDIS_D",   "10"))
TARDIS_K            = int(os.environ.get("TARDIS_K",   "8"))
TARDIS_LOAD_EPS     = float(os.environ.get("TARDIS_LOAD_EPS",     "5.0"))

node_trackers: dict[str, PeakTracker] = defaultdict(lambda: PeakTracker(POLL_INTERVAL, agg="max"))
pod_trackers:  dict[tuple, PeakTracker] = defaultdict(lambda: PeakTracker(POLL_INTERVAL, agg="max"))

# ── TARDIS embeddings ─────────────────────────────────────────────────────────

class TardisProcess:
    """
    One independent TARDIS embedding process (Algorithm 1).

    Maintains a single d-dimensional context vector that executes a random
    walk, and a per-entity embedding that is pulled toward the context on
    each event. Cosine similarity between two embeddings reflects how often
    the corresponding entities have been active in nearby time windows.
    """
    def __init__(self, d: int, alpha: float, beta: float, seed: int):
        self.d     = d
        self.alpha = alpha
        self.beta  = beta
        self.rng   = np.random.default_rng(seed)

        ctx = self.rng.standard_normal(d)
        self.context: np.ndarray = ctx / np.linalg.norm(ctx)
        self.embeddings: dict[str, np.ndarray] = {}

    def _init_embedding(self, entity: str):
        e = self.rng.standard_normal(self.d)
        self.embeddings[entity] = e / np.linalg.norm(e)

    def step(self, active_entities: set[str]):
        """
        One timestep: update embeddings for all active entities, then
        advance the context vector with fresh Gaussian noise.
        """
        sqrt_1ma = math.sqrt(1.0 - self.alpha)
        sqrt_a   = math.sqrt(self.alpha)
        for entity in active_entities:
            if entity not in self.embeddings:
                self._init_embedding(entity)
            e = self.embeddings[entity]
            e = sqrt_1ma * e + sqrt_a * self.context
            norm = np.linalg.norm(e)
            self.embeddings[entity] = e / norm if norm > 0 else e

        noise = self.rng.standard_normal(self.d)
        ctx = (math.sqrt(1.0 - self.beta) * self.context +
               math.sqrt(self.beta) * noise)
        norm = np.linalg.norm(ctx)
        self.context = ctx / norm if norm > 0 else ctx

    def cosine(self, a: str, b: str) -> float:
        ea = self.embeddings.get(a)
        eb = self.embeddings.get(b)
        if ea is None or eb is None:
            return 0.0
        na, nb = np.linalg.norm(ea), np.linalg.norm(eb)
        if na == 0 or nb == 0:
            return 0.0
        return float(np.dot(ea, eb) / (na * nb))


class TardisEnsemble:
    """
    Ensemble of k independent TARDIS processes (Section 3.1).

    Co-occurrence similarity is averaged across processes to reduce
    variance. Each process is seeded differently for independence.
    """
    def __init__(self, d: int, k: int, alpha: float, beta: float):
        self.processes = [TardisProcess(d, alpha, beta, seed=i) for i in range(k)]
        self.forgotten: set[str] = set()

    def step(self, active_entities: set[str]):
        """Advance all k processes by one timestep."""
        effective = active_entities - self.forgotten
        for proc in self.processes:
            proc.step(effective)
        disappeared = self.forgotten - active_entities
        self.forgotten -= disappeared
        if disappeared:
            print(f"  [TARDIS] forgotten pods cleared (gone from scraper): {disappeared}")

    def similarity(self, a: str, b: str) -> float:
        """Average cosine similarity across all k processes."""
        return sum(p.cosine(a, b) for p in self.processes) / len(self.processes)

    def has_embedding(self, entity: str) -> bool:
        return entity in self.processes[0].embeddings

    def rename(self, old: str, new: str):
        for proc in self.processes:
            if old in proc.embeddings:
                proc.embeddings[new] = proc.embeddings.pop(old)
        print(f"  [TARDIS] transferred embedding {old} → {new}")

    def forget(self, entity: str):
        for proc in self.processes:
            proc.embeddings.pop(entity, None)
        self.forgotten.add(entity)
        print(f"  [TARDIS] removed embedding for {entity}")


# global TARDIS ensemble — updated every scrape tick
tardis = TardisEnsemble(
    d=TARDIS_D, k=TARDIS_K,
    alpha=TARDIS_ALPHA, beta=TARDIS_BETA,
)


# ── tracker + TARDIS update ───────────────────────────────────────────────────

def update_trackers(ts: float, node_samples: dict[str, dict]):
    """
    Update peak trackers and TARDIS embeddings from the latest scrape.

    Active pods (CPU% > TARDIS_ACTIVITY_PCT) get their embeddings pulled
    toward the current context vector. The context advances once per call
    regardless of which pods are active, encoding temporal structure.
    """
    active_pods: set[str] = set()

    for node, sample in node_samples.items():
        node_trackers[node].record(ts, sample.get("cpu_pct", 0.0))
        for pod, pct in sample.get("pods", {}).items():
            pod_trackers[(node, pod)].record(ts, pct)
            if pct >= TARDIS_ACTIVITY_PCT:
                active_pods.add(pod)

    tardis.step(active_pods)

    if active_pods:
        print(f"  [TARDIS] active this tick: {active_pods}")
        print(f"  [TARDIS] total embedded: {list(tardis.processes[0].embeddings.keys())}")


# ── TARDIS migration helpers (Algorithm 2) ───────────────────────────────────

def related_pods_on_node(
    node: str,
    victim: str,
    pod_to_node: dict[str, str],
) -> list[str]:
    """
    Return pods co-located on `node` whose TARDIS similarity to `victim`
    exceeds TAU. These are the pods likely to burst at the same time as
    the victim — the embedding similarity group G_h(v) from the paper.
    """
    return [
        pod for pod, n in pod_to_node.items()
        if n == node
        and pod != victim
        and tardis.has_embedding(pod)
        and tardis.similarity(victim, pod) >= TARDIS_TAU
    ]


def related_peak(
    node: str,
    victim: str,
    pod_to_node: dict[str, str],
) -> float:
    """
    Sum of peak loads of pods on `node` that are temporally related to
    `victim`. Used for both victim ranking (higher = more synchronized
    peers, better to migrate) and feasibility checking on destination.
    """
    peers = related_pods_on_node(node, victim, pod_to_node)
    return sum(pod_trackers[(node, p)].peak() for p in peers)


def migration_feasible(
    victim: str,
    dst_node: str,
    pod_to_node: dict[str, str],
    capacity: float,
) -> bool:
    """
    Feasibility check from Algorithm 2 lines 14-18.

    A destination is feasible if:
      1. avg_load[dst] + avg_load[victim] <= capacity  (always required)
      2a. peak_load[dst] + peak_peak[victim] <= capacity  (standard), OR
      2b. peak_load[victim] + RelatedPeak(dst, victim) <= capacity
          (TARDIS relaxation: peaks won't overlap if not in same cluster)

    The TARDIS relaxation allows migrations that peak-load policies would
    reject when the victim's burst pattern differs from pods on the dst node.
    """
    avg_dst    = node_trackers[dst_node].peak()   # reusing peak tracker; swap
                                                   # for avg tracker if available
    avg_victim = pod_trackers.get(
        next((k for k in pod_trackers if k[1] == victim), (None, victim)),
        PeakTracker(POLL_INTERVAL)
    ).peak()

    if avg_dst + avg_victim > capacity:
        return False

    peak_dst    = node_trackers[dst_node].peak()
    peak_victim = avg_victim

    if peak_dst + peak_victim <= capacity:
        return True

    rel_peak = peak_victim + related_peak(dst_node, victim, pod_to_node)
    return rel_peak <= capacity


# ── migration decision (Algorithm 2) ─────────────────────────────────────────

def decide_migration(
    pod_to_node: dict[str, str],
    active_nodes: list[str],
    capacity: float = 100.0,
) -> tuple[str, str, str] | None:
    """
    Returns (victim_pod, src_node, dst_node) or None.

    Follows Algorithm 2:
      Hotspot detection  → standard peak load threshold
      Victim selection   → load binning + TARDIS tie-break (highest RelatedPeak)
      Target selection   → load binning + TARDIS tie-break (lowest RelatedPeak)
      Feasibility        → avg load + peak load with TARDIS relaxation
    """
    loads = {n: node_trackers[n].peak() for n in active_nodes}
    print(f"  Node peak loads: { {n: f'{v:.1f}%' for n, v in loads.items()} }")

    overloaded = [(n, l) for n, l in loads.items() if l > LOAD_THRESHOLD]
    if not overloaded:
        return None

    for src_node, _ in sorted(overloaded, key=lambda x: x[1], reverse=True):

        node_pods = [p for p, n in pod_to_node.items() if n == src_node]
        if not node_pods:
            print(f"  No worker pods on {src_node}")
            continue

        pod_loads = {p: pod_trackers[(src_node, p)].peak() for p in node_pods}

        victim = _select_victim(src_node, node_pods, pod_loads, pod_to_node)
        if victim is None:
            continue

        print(f"  Victim: {victim} "
              f"(load={pod_loads[victim]:.1f}%, "
              f"related_peak={related_peak(src_node, victim, pod_to_node):.1f}%)")

        dst_nodes = [n for n in active_nodes if n != src_node]
        dst_node  = _select_destination(
            victim, dst_nodes, pod_to_node, capacity
        )
        if dst_node is None:
            print(f"  No feasible destination for {victim}, skipping.")
            continue

        print(f"  Destination: {dst_node} "
              f"(load={loads.get(dst_node, 0):.1f}%, "
              f"related_peak_with_victim="
              f"{related_peak(dst_node, victim, pod_to_node):.1f}%)")
        return victim, src_node, dst_node

    return None


def _bin_by_load(
    items: list,
    load_fn,
    eps: float,
) -> list[list]:
    """
    Group items into bins of width `eps` by load value, descending.
    Items within each bin are candidates for tie-breaking.
    """
    if not items:
        return []
    sorted_items = sorted(items, key=load_fn, reverse=True)
    bins: list[list] = []
    current_bin: list = []
    bin_top: float = load_fn(sorted_items[0])
    for item in sorted_items:
        if bin_top - load_fn(item) <= eps:
            current_bin.append(item)
        else:
            bins.append(current_bin)
            current_bin = [item]
            bin_top = load_fn(item)
    if current_bin:
        bins.append(current_bin)
    return bins


def _select_victim(
    src_node: str,
    node_pods: list[str],
    pod_loads: dict[str, float],
    pod_to_node: dict[str, str],
) -> str | None:
    """
    Victim selection from Algorithm 2 lines 7-9.

    Bin pods by load (descending). Within each bin, sort by RelatedPeak
    descending — the pod most synchronized with its co-located peers is
    evicted first, as moving it breaks up the synchronized burst cluster.
    """
    nonzero = [p for p in node_pods if pod_loads[p] > 0]
    if not nonzero:
        print(f"  All pods on {src_node} have zero peak load, skipping")
        return None

    bins = _bin_by_load(nonzero, lambda p: pod_loads[p], TARDIS_LOAD_EPS)

    for bin_pods in bins:
        bin_pods.sort(
            key=lambda p: related_peak(src_node, p, pod_to_node),
            reverse=True,
        )
        return bin_pods[0]

    return None


def _select_destination(
    victim: str,
    dst_nodes: list[str],
    pod_to_node: dict[str, str],
    capacity: float,
) -> str | None:
    """
    Destination selection from Algorithm 2 lines 11-18.

    Bin destination nodes by load (ascending). Within each bin, sort by
    RelatedPeak(dst, victim) ascending — prefer the node whose existing
    pods are least temporally synchronized with the victim, minimizing
    future burst overlap after migration.

    Only return a node that passes the feasibility check (avg + TARDIS
    peak relaxation).
    """
    dst_loads = {n: node_trackers[n].peak() for n in dst_nodes}

    bins = _bin_by_load(
        dst_nodes,
        lambda n: dst_loads[n],
        TARDIS_LOAD_EPS,
    )
    bins = list(reversed(bins))

    for bin_nodes in bins:
        bin_nodes.sort(
            key=lambda n: related_peak(n, victim, pod_to_node),
        )
        for dst in bin_nodes:
            if migration_feasible(victim, dst, pod_to_node, capacity):
                return dst

    return None


# ── main loops ────────────────────────────────────────────────────────────────

current_node_ips:    dict[str, str] = {}
current_pod_to_node: dict[str, str] = {}


async def scrape_loop(session: aiohttp.ClientSession):
    """
    Scrapes collectors every SCRAPE_INTERVAL seconds.
    Also drives TARDIS embedding updates — one step per tick.
    """
    while True:
        if current_node_ips:
            node_samples = await scrape_all(session, current_node_ips)
            update_trackers(time.time(), node_samples)
        await asyncio.sleep(SCRAPE_INTERVAL)


async def discovery_loop():
    """Refreshes node IPs and pod placement every POLL_INTERVAL seconds."""
    global current_node_ips, current_pod_to_node
    while True:
        node_ips = get_collector_pod_ips()
        if node_ips:
            current_node_ips    = node_ips
            current_pod_to_node = get_worker_pods()
        await asyncio.sleep(POLL_INTERVAL)


async def decision_loop():
    """Runs TARDIS-augmented migration decisions every POLL_INTERVAL seconds."""
    node_last_eviction: dict[str, float] = {}

    warmup = POLL_INTERVAL + 1
    print(f"  Waiting {warmup:.0f}s for discovery and TARDIS warm-up...")
    await asyncio.sleep(warmup)

    while True:
        print(f"\n[{time.strftime('%H:%M:%S')}] TARDIS controller tick")

        if not current_node_ips:
            print("  No collector pods found, skipping.")
            await asyncio.sleep(POLL_INTERVAL)
            continue

        now     = time.time()
        cooling = {n for n, t in node_last_eviction.items()
                   if now - t < POLL_INTERVAL}
        if cooling:
            print(f"  Nodes in cooldown: {cooling}")

        active_nodes = [n for n in current_node_ips if n not in cooling]

        n_pods     = len(current_pod_to_node)
        n_embedded = sum(
            1 for p in current_pod_to_node if tardis.has_embedding(p)
        )
        print(f"  TARDIS coverage: {n_embedded}/{n_pods} pods have embeddings")

        decision = decide_migration(
            current_pod_to_node,
            active_nodes,
            capacity=LOAD_THRESHOLD,
        )

        if not decision:
            print("  No migrations needed.")
            await asyncio.sleep(POLL_INTERVAL)
            continue

        pod, src_node, dst_node = decision

        def _rename_hook(old_pod, new_pod_name, old_pod_data):
            tardis.rename(old_pod, new_pod_name)

        def _forget_hook(old_pod):
            tardis.forget(old_pod)

        success = run_migration(
            pod, dst_node,
            pre_drain_hook=_rename_hook,
            post_delete_hook=_forget_hook,
        )
        if success:
            node_last_eviction[src_node] = time.time()

        await asyncio.sleep(POLL_INTERVAL)


async def control_loop():
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            scrape_loop(session),
            discovery_loop(),
            decision_loop(),
        )


def main():
    print(
        f"TARDIS controller starting: "
        f"threshold={LOAD_THRESHOLD}% "
        f"poll={POLL_INTERVAL}s "
        f"scrape={SCRAPE_INTERVAL}s\n"
        f"  TARDIS: alpha={TARDIS_ALPHA} beta={TARDIS_BETA} "
        f"tau={TARDIS_TAU} activity_pct={TARDIS_ACTIVITY_PCT}% "
        f"d={TARDIS_D} k={TARDIS_K} eps={TARDIS_LOAD_EPS}"
    )
    asyncio.run(control_loop())


if __name__ == "__main__":
    main()