#!/usr/bin/env python3
import csv
import os
import pickle
import argparse
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _to_date_str(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def _find_ticker_dirs(base_dir: Path, ticker: Optional[str]) -> List[Path]:
    if ticker:
        target = base_dir / ticker
        if not target.exists():
            raise FileNotFoundError(f"Ticker directory not found: {target}")
        return [target]

    dirs: List[Path] = []
    if (base_dir / "agent_1" / "state_dict.pkl").exists():
        return [base_dir]

    for child in sorted(base_dir.iterdir()):
        if not child.is_dir():
            continue
        if (child / "agent_1" / "state_dict.pkl").exists():
            dirs.append(child)
    return dirs


def _load_state_dict(state_dict_path: Path) -> Dict[str, Any]:
    with state_dict_path.open("rb") as f:
        return pickle.load(f)


def _extract_actions_rows(
    portfolio: Any,
) -> List[Tuple[str, str, Optional[int]]]:
    symbol = getattr(portfolio, "symbol", "")
    action_series = getattr(portfolio, "action_series", {})

    rows: List[Tuple[str, str, Optional[int]]] = []
    for cur_date in sorted(action_series.keys()):
        rows.append((_to_date_str(cur_date), symbol, action_series.get(cur_date)))
    return rows


def _extract_decision_rows(
    reflection_result_series_dict: Dict[Any, Dict[str, Any]],
    action_series: Dict[Any, int],
) -> List[Tuple[str, Optional[str], Optional[str], Optional[int]]]:
    rows: List[Tuple[str, Optional[str], Optional[str], Optional[int]]] = []
    for cur_date in sorted(reflection_result_series_dict.keys()):
        payload = reflection_result_series_dict.get(cur_date) or {}
        rows.append(
            (
                _to_date_str(cur_date),
                payload.get("investment_decision"),
                payload.get("summary_reason"),
                action_series.get(cur_date),
            )
        )
    return rows


def _direction_to_label(direction: Optional[int]) -> str:
    if direction == 1:
        return "BUY"
    if direction == -1:
        return "SELL"
    return "HOLD"


def _normalize_decision_label(decision: Optional[str], direction: Optional[int]) -> str:
    if isinstance(decision, str):
        cleaned = decision.strip().lower()
        if cleaned == "buy":
            return "BUY"
        if cleaned == "sell":
            return "SELL"
        if cleaned == "hold":
            return "HOLD"
    return _direction_to_label(direction)


def _extract_backtest_decision_rows(
    symbol: str,
    reflection_result_series_dict: Dict[Any, Dict[str, Any]],
    action_series: Dict[Any, int],
) -> List[Tuple[str, str, str]]:
    rows: List[Tuple[str, str, str]] = []
    for cur_date in sorted(reflection_result_series_dict.keys()):
        payload = reflection_result_series_dict.get(cur_date) or {}
        if "investment_decision" not in payload:
            continue
        date_str = _to_date_str(cur_date)
        decision_label = _normalize_decision_label(
            payload.get("investment_decision"),
            action_series.get(cur_date),
        )
        rows.append((symbol, date_str, decision_label))
    return rows


def _write_csv(
    path: Path, headers: Iterable[str], rows: Iterable[Tuple[Any, ...]]
) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(list(headers))
        writer.writerows(rows)


def _process_ticker_dir(ticker_dir: Path, output_dir: Optional[Path]) -> Dict[str, str]:
    state_dict_path = ticker_dir / "agent_1" / "state_dict.pkl"
    state_dict = _load_state_dict(state_dict_path)

    portfolio = state_dict["portfolio"]
    reflection = state_dict.get("reflection_result_series_dict", {})
    action_series = getattr(portfolio, "action_series", {})
    symbol = getattr(portfolio, "symbol", ticker_dir.name)

    ticker_name = ticker_dir.name
    out_dir = output_dir if output_dir else ticker_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    actions_path = out_dir / f"actions_{ticker_name}.csv"
    legacy_decisions_path = out_dir / f"decisions_{ticker_name}.csv"

    action_rows = _extract_actions_rows(portfolio)
    decision_rows = _extract_decision_rows(reflection, action_series)
    backtest_rows = _extract_backtest_decision_rows(symbol, reflection, action_series)

    if not backtest_rows:
        raise ValueError("No test-mode investment decisions found")

    start_date = backtest_rows[0][1]
    end_date = backtest_rows[-1][1]
    backtest_decisions_path = (
        out_dir / f"{symbol}_decisions_{start_date}_{end_date}_finmem.csv"
    )

    _write_csv(actions_path, ["date", "symbol", "direction"], action_rows)
    _write_csv(
        legacy_decisions_path,
        ["date", "investment_decision", "summary_reason", "direction"],
        decision_rows,
    )
    _write_csv(
        backtest_decisions_path,
        ["stock", "test_date", "decision"],
        backtest_rows,
    )

    return {
        "ticker": ticker_name,
        "actions": str(actions_path),
        "decisions": str(backtest_decisions_path),
        "legacy_decisions": str(legacy_decisions_path),
        "n_actions": str(len(action_rows)),
        "n_decisions": str(len(backtest_rows)),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export FinMem checkpoint results to CSV files."
    )
    parser.add_argument(
        "--base-dir",
        default=os.path.join("data", "07_test_model_output"),
        help="Base directory containing ticker result folders.",
    )
    parser.add_argument(
        "--ticker",
        default=None,
        help="Optional ticker folder name to export only one ticker.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional output directory. Defaults to each ticker directory.",
    )
    args = parser.parse_args()

    base_dir = Path(args.base_dir)
    if not base_dir.exists():
        raise FileNotFoundError(f"Base directory not found: {base_dir}")

    output_dir = Path(args.output_dir) if args.output_dir else None
    ticker_dirs = _find_ticker_dirs(base_dir, args.ticker)
    if not ticker_dirs:
        raise FileNotFoundError(f"No valid checkpoint folders found under {base_dir}")

    summaries = []
    for ticker_dir in ticker_dirs:
        try:
            summary = _process_ticker_dir(ticker_dir=ticker_dir, output_dir=output_dir)
            summaries.append(summary)
        except Exception as e:
            print(f"[WARN] Failed {ticker_dir.name}: {e}")

    if not summaries:
        print("[WARN] No ticker results were exported.")
        return

    print("Export completed:")
    for item in summaries:
        print(
            f"- {item['ticker']}: actions={item['n_actions']} -> {item['actions']}; "
            f"decisions={item['n_decisions']} -> {item['decisions']}"
        )


if __name__ == "__main__":
    main()
