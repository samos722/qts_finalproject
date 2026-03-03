---
marp: true
theme: default
paginate: true
---

# Crypto Perpetual Futures Market Neutral Strategy

**Team Members**: Samos Zhu (12498135), Wenxin Chang (12497798), Henry Xu (12284734), Cody Torgovnik (12496679)

---

## 1. Project Motivation

- **Crypto perpetual futures** have a unique structure: funding rates paid every 8 hours between longs and shorts
- When funding is **high** → longs pay shorts → crowded long side → potential mean reversion
- When funding is **low/negative** → shorts pay longs → crowded short side → potential squeeze
- **Idea**: Harvest funding carry while staying **market-neutral** (long low-funding, short high-funding)

---

## 2. Why Funding Rates Matter

- **Funding rate** = periodic cash flow between long and short positions
- Settles every 8h at 00:00, 08:00, 16:00 UTC
- **Positive funding**: longs pay shorts (bullish sentiment, crowded longs)
- **Negative funding**: shorts pay longs (bearish sentiment, crowded shorts)
- Market-neutral: we can **receive funding** on both sides by choosing the right longs and shorts

---

## 3. Why Crowding (Volume Change) Matters

- High funding alone can be a trap if it reflects true supply/demand
- **Crowding proxy**: volume change (成交量变化) — OI not on data.binance.vision
- Logic: Funding **high** + volume **rising** → crowded → fade (short)
- Funding **low** + not crowded → potential long (receive funding, bet on mean reversion)

---

## 4. Strategy Structure (8h / 1h / 5m)

- **8h Alpha**: Rebalance Long Bottom-K (low signal) / Short Top-K (high signal)
- **1h Risk**: Target-vol scaling, drawdown gates
- **5m Tail**: Event brake on sharp moves (reduce/flat on large intraday moves)

---

## 5. Data & Draft Status

- **Data source**: data.binance.vision (klines + funding)
- **Crowding**: volume_change (成交量变化) as proxy — OI not on vision
- **Draft**: Data pipeline, signal framework in place; performance analysis in final submission
