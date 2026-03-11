# Dark Store Simulation — Technical Methodology

## Overview

This document describes the decision logic, mathematics, and operational behaviour of the **Conditional Heuristic Ranking Engine** embedded in `dark_store_sim.py`.

---

## 1. Order Lifecycle

```
Arrival → [Picker Free?] ─YES─→ Instant Assignment (FCFS)
                │
               NO
                ↓
          Pending Pool
                │
    [Pool ETC > 3 min?] ─NO─→ Serve in Arrival Order (FCFS)
                │
               YES
                ↓
    Sort Pool by Lowest ETC First (Heuristic)
```

---

## 2. The 3-Minute Backlog Threshold

### Why 3 minutes?
Three minutes represents the point at which chronological queuing starts to create measurable SLA risk. Below this ceiling the overhead of sorting is not worth the marginal gain; above it, reordering the pool by complexity can recover significant throughput.

### Calculation
At every dispatch decision the system computes:

```
Pool_ETC_Total = Σ ETC(order_i)   for all orders in Pending Pool
```

| Pool_ETC_Total | Behaviour |
|---|---|
| < 180 s | **Normal — FCFS.** Orders dispatched in arrival order. |
| ≥ 180 s | **Optimised — Heuristic Active.** Pool re-sorted ascending by ETC before next dispatch. |

The threshold value is read from `ranking_policy.json → backlog_threshold_seconds` and can be tuned without code changes.

---

## 3. ETC (Estimated Time to Complete) Formula

For a single order `O` containing SKU lines `[s₁, s₂, … sₙ]`:

```
ETC(O) = Base_Pick_Time + Row_Change_Penalties + Cooler_Penalties + Restock_Penalty
```

### 3.1 Base Pick Time
```
Base_Pick_Time = N_skus × sku_process_time
```
- `N_skus` — number of SKU lines in the order  
- `sku_process_time` — seconds to locate, scan, and bag one unit (default **8.5 s**, from policy)

### 3.2 Row-Change Penalty
Each unique *aisle transition* in the optimised pick path adds travel overhead:

```
unique_rows     = set of distinct row letters across all SKU locations in O
N_row_changes   = len(unique_rows) - 1          # transitions between rows
Row_Change_Penalties = N_row_changes × row_change_penalty
```
- `row_change_penalty` — default **12.0 s** per crossing

*Example:* An order touching rows A, C, F has 2 transitions → +24 s penalty.

### 3.3 Cooler-Row Constant
Orders that include any SKU located in a refrigerated / freezer aisle (rows defined in `cooler_rows`, default **C** and **D**) incur a fixed handling surcharge per cooler row touched:

```
cooler_rows_in_order  = unique_rows ∩ cooler_rows
Cooler_Penalties = len(cooler_rows_in_order) × cooler_row_constant
```
- `cooler_row_constant` — default **15.0 s** per cooler row (for PPE donning, door open/close, temperature compliance)

### 3.4 Restocking Penalty — One-End Access Model
When a row is flagged **Restocking**, the aisle is partially blocked; a picker can only enter from one end rather than walking straight through. This doubles effective travel distance for SKUs deep in that aisle.

```
restock_rows_in_order = unique_rows ∩ currently_restocking_rows

IF restock_rows_in_order is non-empty:
    ETC(O) = ETC(O) × restock_delay_multiplier
```
- `restock_delay_multiplier` — default **2.8×**
- The multiplier is applied **once to the running total**, not per row, to reflect the dominating effect of a single blocked aisle forcing a full backtrack.

### 3.5 Complete Formula (pseudo-code)
```python
def calc_etc(order, policy, restocking_rows):
    n   = len(order.skus)
    rows = {sku.row for sku in order.skus}

    base      = n * policy["sku_process_time"]
    row_pen   = max(0, len(rows) - 1) * policy["row_change_penalty"]
    cool_pen  = len(rows & set(policy["cooler_rows"])) * policy["cooler_row_constant"]

    etc = base + row_pen + cool_pen

    if rows & restocking_rows:          # one-end access hit
        etc *= policy["restock_delay_multiplier"]

    return round(etc, 1)
```

---

## 4. One-End Access During Restocking

### Physical Model
A standard dark-store aisle allows bi-directional traversal. During a restock operation, a pallet jack or cage blocks the mid-aisle access point, leaving only the entry end clear. A picker who needs an SKU past the blockage must:

1. Enter from the open end.
2. Walk to the blockage limit.
3. **Reverse back out** and circle to the far end if the SKU is beyond the block.

This worst-case path is approximately **2×** the normal one-way distance, and when combined with slower movement around equipment the effective multiplier lands near **2.8×** (empirically derived; adjustable in policy).

### Simulation Behaviour
- Each row has a random `restock_duration` (60–180 s) assigned when a restock event fires.
- The grid highlights restocking rows in **RED**.
- Any pending order whose SKU set intersects a restocking row has its ETC recalculated at dispatch time (not at arrival) so real-time aisle state is always reflected.

---

## 5. Picker Assignment

- The simulation maintains a fixed pool of **N pickers** (default 3).
- A picker is *free* when their current order timer reaches zero.
- On each simulation tick:
  1. Free pickers are identified.
  2. If `Pending Pool` is non-empty:
     - Compute `Pool_ETC_Total`.
     - If ≥ threshold → sort pool ascending by ETC.
     - Pop the head of the (possibly re-sorted) pool and assign to picker.
  3. Picker position on the grid is animated along the SKU coordinate sequence.

---

## 6. Policy File Reference (`ranking_policy.json`)

| Key | Type | Default | Effect |
|---|---|---|---|
| `sku_process_time` | float (s) | 8.5 | Per-SKU base cost |
| `row_change_penalty` | float (s) | 12.0 | Per aisle transition |
| `restock_delay_multiplier` | float | 2.8 | Whole-order multiplier when restock hit |
| `cooler_row_constant` | float (s) | 15.0 | Per cooler row touched |
| `backlog_threshold_seconds` | int (s) | 180 | Pool ETC trigger for heuristic |
| `cooler_rows` | list[str] | ["C","D"] | Row labels with cooler surcharge |

All values can be edited in `ranking_policy.json` without modifying Python source. The app reloads the policy on each simulation reset.

---

## 7. UI Indicators

| Indicator | Condition | Colour |
|---|---|---|
| **Normal (FCFS)** | Pool ETC < 3 min | Green |
| **Optimised (Heuristic Ranking)** | Pool ETC ≥ 3 min | Amber/Red |
| Grid row highlight RED | Row currently restocking | Red |
| Picker marker | Moves each tick along pick path | Cyan dot |

---

*Document version 1.0 — matches `dark_store_sim.py` v1.0 and `ranking_policy.json` v1.0.0*
