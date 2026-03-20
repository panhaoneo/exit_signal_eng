#!/usr/bin/env python3
"""
Exit Signal Engine — Data Fetcher
Run by GitHub Actions daily. Outputs docs/signals.json.
"""

import json
import datetime
import sys
import yfinance as yf
import pandas as pd

TICKERS = {
    "zijin": "601899.SS",   # 紫金矿业 A股
    "gold":  "GC=F",        # 黄金期货
    "gdx":   "GDX",         # VanEck Gold Miners ETF
    "copper":"HG=F",        # 铜期货
    "vix":   "^VIX",        # VIX
}

MA_WINDOW = 20
VIX_THRESHOLD = 25
BASE_POSITION = 80          # 假设基准仓位 80%

def fetch_history(period="60d") -> dict[str, pd.Series]:
    """下载各标的收盘价，返回 {key: Series}"""
    data = {}
    for key, ticker in TICKERS.items():
        try:
            df = yf.download(ticker, period=period, auto_adjust=True, progress=False)
            if df.empty:
                print(f"[WARN] {ticker} 返回空数据", file=sys.stderr)
                data[key] = pd.Series(dtype=float)
            else:
                data[key] = df["Close"].squeeze()
        except Exception as e:
            print(f"[ERROR] {ticker}: {e}", file=sys.stderr)
            data[key] = pd.Series(dtype=float)
    return data

def align(series_dict: dict[str, pd.Series]) -> pd.DataFrame:
    """对齐时间轴，前向填充（处理 A股/美股交易日差异）"""
    df = pd.DataFrame(series_dict)
    df = df.ffill().dropna()
    return df

def compute_signals(df: pd.DataFrame) -> dict:
    if len(df) < MA_WINDOW:
        raise ValueError(f"数据不足 {MA_WINDOW} 行，无法计算均线")

    r = df.iloc[-1]        # 最新一行
    prev = df.iloc[-2]     # 前一行（判断方向）

    # --- 比率 ---
    zijin_gold_ratio = df["zijin"] / df["gold"]
    gdx_gold_ratio   = df["gdx"]   / df["gold"]
    copper_gold_ratio= df["copper"]/ df["gold"]

    # --- 20日均线 ---
    zg_ma  = zijin_gold_ratio.rolling(MA_WINDOW).mean().iloc[-1]
    gg_ma  = gdx_gold_ratio.rolling(MA_WINDOW).mean().iloc[-1]
    cg_ma  = copper_gold_ratio.rolling(MA_WINDOW).mean().iloc[-1]
    gold_ma= df["gold"].rolling(MA_WINDOW).mean().iloc[-1]
    gdx_ma = df["gdx"].rolling(MA_WINDOW).mean().iloc[-1]

    # --- 信号 ---
    signals = {
        "zijin_weakness":   bool(zijin_gold_ratio.iloc[-1]  < zg_ma),
        "gdx_weakness":     bool(gdx_gold_ratio.iloc[-1]    < gg_ma),
        "copper_breakdown": bool(copper_gold_ratio.iloc[-1] < cg_ma),
        "divergence":       bool(r["gold"] > gold_ma and r["gdx"] < gdx_ma),
        "risk_spike":       bool(r["vix"]  > VIX_THRESHOLD),
    }

    score = sum(signals.values())

    # --- 操作建议 ---
    if score >= 4:
        action, reduce = "快速撤退", 0.75
    elif score >= 3:
        action, reduce = "减仓 50%", 0.50
    elif score >= 2:
        action, reduce = "减仓 20%", 0.20
    else:
        action, reduce = "正常持有", 0.00

    suggested_position = round(BASE_POSITION * (1 - reduce))

    # --- 原始值（展示用）---
    raw = {
        "zijin":             round(float(r["zijin"]), 3),
        "gold":              round(float(r["gold"]),  2),
        "gdx":               round(float(r["gdx"]),   2),
        "copper":            round(float(r["copper"]),4),
        "vix":               round(float(r["vix"]),   2),
        "zijin_gold_ratio":  round(float(zijin_gold_ratio.iloc[-1]),  6),
        "gdx_gold_ratio":    round(float(gdx_gold_ratio.iloc[-1]),    6),
        "copper_gold_ratio": round(float(copper_gold_ratio.iloc[-1]), 6),
        "zijin_gold_ratio_ma20":  round(float(zg_ma),  6),
        "gdx_gold_ratio_ma20":    round(float(gg_ma),  6),
        "copper_gold_ratio_ma20": round(float(cg_ma),  6),
        "gold_ma20":         round(float(gold_ma), 2),
        "gdx_ma20":          round(float(gdx_ma),  2),
    }

    # --- 历史评分（最近 30 日，用于趋势图）---
    history = []
    tail = df.tail(30)
    zg_roll  = zijin_gold_ratio.rolling(MA_WINDOW).mean()
    gg_roll  = gdx_gold_ratio.rolling(MA_WINDOW).mean()
    cg_roll  = copper_gold_ratio.rolling(MA_WINDOW).mean()
    gm_roll  = df["gold"].rolling(MA_WINDOW).mean()
    gdxm_roll= df["gdx"].rolling(MA_WINDOW).mean()

    for idx in tail.index:
        row = df.loc[idx]
        s = {
            "zijin_weakness":   bool(zijin_gold_ratio.loc[idx]  < zg_roll.loc[idx])  if not pd.isna(zg_roll.loc[idx])  else False,
            "gdx_weakness":     bool(gdx_gold_ratio.loc[idx]    < gg_roll.loc[idx])  if not pd.isna(gg_roll.loc[idx])  else False,
            "copper_breakdown": bool(copper_gold_ratio.loc[idx] < cg_roll.loc[idx])  if not pd.isna(cg_roll.loc[idx])  else False,
            "divergence":       bool(row["gold"] > gm_roll.loc[idx] and row["gdx"] < gdxm_roll.loc[idx]) if not pd.isna(gm_roll.loc[idx]) else False,
            "risk_spike":       bool(row["vix"]  > VIX_THRESHOLD),
        }
        history.append({
            "date":  idx.strftime("%Y-%m-%d"),
            "score": sum(s.values()),
        })

    return {
        "updated_at": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "score":      score,
        "action":     action,
        "base_position":      BASE_POSITION,
        "suggested_position": suggested_position,
        "signals":    signals,
        "raw":        raw,
        "history":    history,
    }

def main():
    print("Fetching market data...", file=sys.stderr)
    series = fetch_history(period="60d")
    df = align(series)
    print(f"Aligned rows: {len(df)}", file=sys.stderr)

    result = compute_signals(df)

    output_path = "signals.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"Written to {output_path}", file=sys.stderr)
    print(json.dumps(result, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
