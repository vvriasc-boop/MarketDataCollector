#!/usr/bin/env python3
"""
L/S + Taker Backtest — SHORT strategy.

Ищет момент, когда L/S аномально высок для данного символа (адаптивный порог:
mean + K*σ) И Taker < 1.0 одновременно. Симулирует SHORT вход по mark_price.
Сравнивает с OI Flush стратегией.

Запуск: python3 backtest_ls_taker.py
"""

import io
import os
import sqlite3
import statistics
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

from dotenv import load_dotenv

from backtest_oi_flush import (
    Tee, send_to_telegram, build_price_path, simulate_trade,
    load_symbols, load_oi_data, load_nearest_metric,
    ts_to_str, fmt_pnl, fmt_price, fmt_exit,
    find_signals as find_oi_flush_signals,
    MIN_HISTORY,
)
from optimizer import run_optimization

load_dotenv()

# ═══════════════════════════════════════════════════════
# НАСТРАИВАЕМЫЕ ПАРАМЕТРЫ
# ═══════════════════════════════════════════════════════

DB_PATH = os.getenv("DB_PATH", "market_data.db")

# Пороги сигнала — L/S адаптивный (per-symbol)
LS_ZSCORE = 2.0              # σ выше среднего для порога
LS_MIN_ABS = 1.5             # абсолютный минимум (не ниже этого)
LS_MIN_DATAPOINTS = 24       # мин точек для расчёта stats (2ч)
TAKER_THRESHOLD = 1.0        # фиксированный

# Дедупликация
SIGNAL_COOLDOWN = 6         # точек между сигналами одной пары (30 мин)

# Симуляция сделки
TAKE_PROFIT = 3.0           # %
STOP_LOSS = 2.0             # %
MAX_HOLD_POINTS = 0         # 0 = без лимита, держать до TP/SL/конца данных

# Интервал
POINT_INTERVAL = 300        # 5 мин


# ═══════════════════════════════════════════════════════
# ПОИСК СИГНАЛОВ L/S + TAKER
# ═══════════════════════════════════════════════════════

def compute_ls_thresholds(conn):
    """Адаптивные пороги L/S per symbol: mean + LS_ZSCORE * σ."""
    cur = conn.execute(
        "SELECT symbol_id, ratio FROM long_short_ratio ORDER BY symbol_id"
    )
    rows = cur.fetchall()

    by_symbol = defaultdict(list)
    for sym_id, ratio in rows:
        by_symbol[sym_id].append(ratio)

    thresholds = {}
    for sym_id, ratios in by_symbol.items():
        if len(ratios) < LS_MIN_DATAPOINTS:
            continue
        mean = statistics.mean(ratios)
        stdev = statistics.stdev(ratios) if len(ratios) > 1 else 0.0
        adaptive = mean + LS_ZSCORE * stdev
        thresholds[sym_id] = {
            'mean': mean,
            'stdev': stdev,
            'adaptive': adaptive,
            'threshold': max(adaptive, LS_MIN_ABS),
            'count': len(ratios),
        }
    return thresholds


def find_ls_taker_signals(conn, symbol_id, ls_threshold):
    """Timestamps where L/S > adaptive threshold AND Taker < threshold."""
    cur = conn.execute(
        """SELECT ls.timestamp, ls.ratio, t.buy_sell_ratio
           FROM long_short_ratio ls
           INNER JOIN taker_ratio t
               ON ls.symbol_id = t.symbol_id AND ls.timestamp = t.timestamp
           WHERE ls.symbol_id = ?
             AND ls.ratio > ?
             AND t.buy_sell_ratio < ?
           ORDER BY ls.timestamp ASC""",
        (symbol_id, ls_threshold, TAKER_THRESHOLD),
    )
    return cur.fetchall()


# ═══════════════════════════════════════════════════════
# СТАТИСТИКА И СРАВНЕНИЕ
# ═══════════════════════════════════════════════════════

def _print_stats(closed_trades, open_trades, total_signals):
    """Вывести сводную статистику по закрытым сделкам."""
    print("───── ИТОГИ ─────\n")
    print(f"Всего сигналов: {total_signals}")
    print(f"Закрытых сделок: {len(closed_trades)} ({open_trades} ещё открыты)")

    if not closed_trades:
        return

    exit_counts = defaultdict(int)
    for t in closed_trades:
        exit_counts[t['exit_type']] += 1

    print("\nПо выходам:")
    for etype in ['TP', 'SL', 'TIMEOUT']:
        cnt = exit_counts.get(etype, 0)
        if cnt > 0:
            print(f"  {etype:8s} {cnt:3d} ({cnt / len(closed_trades) * 100:.0f}%)")

    total_pnl = sum(t['pnl_pct'] for t in closed_trades)
    avg_pnl = total_pnl / len(closed_trades)
    winners = [t for t in closed_trades if t['pnl_pct'] > 0]
    win_rate = len(winners) / len(closed_trades) * 100
    best = max(closed_trades, key=lambda t: t['pnl_pct'])
    worst = min(closed_trades, key=lambda t: t['pnl_pct'])

    print(f"\nP&L:")
    print(f"  Общий:    {fmt_pnl(total_pnl)}")
    print(f"  Средний:  {fmt_pnl(avg_pnl)} на сделку")
    print(f"  Лучшая:   {best['symbol']} {fmt_pnl(best['pnl_pct'])}")
    print(f"  Худшая:   {worst['symbol']} {fmt_pnl(worst['pnl_pct'])}")
    print(f"  Win rate: {win_rate:.0f}%")

    # ТОП пары
    pair_stats = defaultdict(lambda: {'count': 0, 'wins': 0, 'pnl': 0.0})
    for t in closed_trades:
        pair_stats[t['symbol']]['count'] += 1
        pair_stats[t['symbol']]['pnl'] += t['pnl_pct']
        if t['pnl_pct'] > 0:
            pair_stats[t['symbol']]['wins'] += 1

    top_pairs = sorted(pair_stats.items(), key=lambda x: x[1]['count'], reverse=True)
    print("\nТОП пары по кол-ву сигналов:")
    for sym, st in top_pairs[:10]:
        wr = st['wins'] / st['count'] * 100 if st['count'] > 0 else 0
        print(f"  {sym:15s} {st['count']:2d} сигналов, win rate {wr:.0f}%, "
              f"P&L {fmt_pnl(st['pnl'])}")


def _calc_strategy_stats(signals_list):
    """Рассчитать статистику для списка сигналов с trade."""
    closed = [s for s in signals_list if s.get('trade') is not None]
    if not closed:
        return None
    results = [s['trade']['pnl_pct'] for s in closed]
    total = sum(results)
    wins = sum(1 for r in results if r > 0)
    return {
        'trades': len(results),
        'wins': wins,
        'win_rate': wins / len(results) * 100,
        'total_pnl': total,
        'avg_pnl': total / len(results),
    }


def _print_comparison(closed_trades, oi_flush_signals, hours):
    """Таблица сравнения стратегий + автозаключение."""
    print("\n\n═══ СРАВНЕНИЕ СТРАТЕГИЙ ═══\n")
    print(f"(TP={TAKE_PROFIT}%, SL={STOP_LOSS}% для всех стратегий)\n")

    strategies = []

    # 1. L/S + Taker (этот скрипт)
    if closed_trades:
        total = sum(t['pnl_pct'] for t in closed_trades)
        wins = sum(1 for t in closed_trades if t['pnl_pct'] > 0)
        n = len(closed_trades)
        strategies.append(("L/S + Taker (без OI)", n, wins,
                           wins / n * 100, total, total / n))

    # 2. OI Flush (все сигналы)
    oi_stats = _calc_strategy_stats(oi_flush_signals)
    if oi_stats:
        strategies.append(("OI Flush (все)", oi_stats['trades'], oi_stats['wins'],
                           oi_stats['win_rate'], oi_stats['total_pnl'],
                           oi_stats['avg_pnl']))

    # 3. OI Flush + L/S > 2.0 + Taker < 1.0 (фикс. порог для сравнения)
    oi_lt = [s for s in oi_flush_signals
             if s.get('ls_ratio') is not None and s['ls_ratio'] > LS_MIN_ABS
             and s.get('taker_ratio') is not None and s['taker_ratio'] < TAKER_THRESHOLD]
    oi_lt_stats = _calc_strategy_stats(oi_lt)
    if oi_lt_stats:
        strategies.append(("OI Flush + L/S + Taker", oi_lt_stats['trades'],
                           oi_lt_stats['wins'], oi_lt_stats['win_rate'],
                           oi_lt_stats['total_pnl'], oi_lt_stats['avg_pnl']))

    if not strategies:
        print("Нет данных для сравнения")
        return

    print("┌─────────────────────────┬────────┬──────┬──────────┬──────────┬────────────┐")
    print("│ Стратегия               │ Сделок │ Wins │ Win rate │ Общий P&L│ Avg P&L    │")
    print("├─────────────────────────┼────────┼──────┼──────────┼──────────┼────────────┤")
    for name, trades, wins, wr, total, avg in strategies:
        print(f"│ {name:23s} │ {trades:6d} │ {wins:4d} │ {wr:5.0f}%   │ "
              f"{fmt_pnl(total):8s} │ {fmt_pnl(avg):10s} │")
    print("└─────────────────────────┴────────┴──────┴──────────┴──────────┴────────────┘")

    # ─── АВТОЗАКЛЮЧЕНИЕ ───────────────────────────────

    print("\n───── ЗАКЛЮЧЕНИЕ ─────\n")

    lt_row = strategies[0] if strategies else None
    oi_lt_row = None
    for s in strategies:
        if "OI Flush + L/S" in s[0]:
            oi_lt_row = s
            break

    if lt_row and oi_lt_row:
        _, lt_n, _, lt_wr, _, lt_avg = lt_row
        _, oilt_n, _, oilt_wr, _, oilt_avg = oi_lt_row

        if oilt_wr > lt_wr + 5 and oilt_avg > lt_avg:
            print("✅ OI Flush фильтр УЛУЧШАЕТ результат:")
            print(f"   Win rate: {lt_wr:.0f}% → {oilt_wr:.0f}% (+{oilt_wr - lt_wr:.0f}pp)")
            print(f"   Avg P&L:  {fmt_pnl(lt_avg)} → {fmt_pnl(oilt_avg)}")
            print(f"   Сделок:   {lt_n} → {oilt_n}")
            if oilt_n < lt_n * 0.3:
                print("   ⚠️  Но кол-во сделок сильно падает — возможен overfitting")
        elif lt_wr >= oilt_wr or lt_avg >= oilt_avg:
            print("❌ OI Flush фильтр НЕ улучшает результат:")
            print(f"   L/S+Taker:          win rate {lt_wr:.0f}%, avg {fmt_pnl(lt_avg)}, "
                  f"{lt_n} сделок")
            print(f"   OI Flush+L/S+Taker: win rate {oilt_wr:.0f}%, avg {fmt_pnl(oilt_avg)}, "
                  f"{oilt_n} сделок")
            if lt_n > oilt_n * 2:
                print("   L/S+Taker даёт больше сделок при сопоставимом качестве")
        else:
            print("≈ Результаты сопоставимы:")
            print(f"   L/S+Taker:          win rate {lt_wr:.0f}%, avg {fmt_pnl(lt_avg)}, "
                  f"{lt_n} сделок")
            print(f"   OI Flush+L/S+Taker: win rate {oilt_wr:.0f}%, avg {fmt_pnl(oilt_avg)}, "
                  f"{oilt_n} сделок")
    elif lt_row and not oi_lt_row:
        print("OI Flush + L/S + Taker: нет совпадающих сделок")
        print("L/S + Taker работает самостоятельно без OI Flush фильтра")
    else:
        print("Недостаточно данных для выводов")

    if hours < 168:
        print(f"\n⚠️  Данные: {hours:.0f}ч — выводы предварительные, нужна неделя+")


# ═══════════════════════════════════════════════════════
# ВЫВОД В ФАЙЛ + TELEGRAM
# ═══════════════════════════════════════════════════════

def _save_and_send(buf, old_stdout):
    """Восстановить stdout, сохранить в .txt, отправить в TG."""
    sys.stdout = old_stdout
    result_text = buf.getvalue()
    if not result_text.strip():
        return
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M")
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            f"backtest_ls_taker_{date_str}.txt")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(result_text)
    print(f"💾 Сохранено: {out_path}")
    send_to_telegram(out_path)


# ═══════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════

def main():
    start_time = time.time()
    buf = io.StringIO()
    old_stdout = sys.stdout
    sys.stdout = Tee(old_stdout, buf)
    try:
        _main_impl(start_time)
    finally:
        _save_and_send(buf, old_stdout)


def _main_impl(start_time):
    if not os.path.exists(DB_PATH):
        print(f"БД не найдена: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = None

    symbols = load_symbols(conn)
    if not symbols:
        print("Нет активных пар в БД")
        conn.close()
        return

    cur = conn.execute("SELECT MIN(timestamp), MAX(timestamp) FROM open_interest")
    row = cur.fetchone()
    if not row or row[0] is None:
        print("Нет данных в open_interest")
        conn.close()
        return

    db_min_ts, db_max_ts = row[0], row[1]

    # ─── АДАПТИВНЫЕ ПОРОГИ L/S ────────────────────────

    ls_thresholds = compute_ls_thresholds(conn)

    # ─── СБОР СИГНАЛОВ (L/S+Taker + OI Flush за один проход) ──

    all_signals = []          # L/S + Taker сигналы
    oi_flush_signals = []     # OI Flush сигналы (для сравнения)
    pairs_with_lt_signals = 0

    for sym_id, sym_name in symbols.items():
        oi_points = load_oi_data(conn, sym_id)
        if len(oi_points) < MIN_HISTORY:
            continue

        # Индекс OI по timestamp
        oi_ts_to_idx = {ts: idx for idx, (ts, _, _) in enumerate(oi_points)}

        # --- L/S + Taker сигналы ---
        ls_info = ls_thresholds.get(sym_id)
        if ls_info is None:
            # Нет данных L/S — пропускаем L/S+Taker, но OI Flush ниже всё равно ищем
            hits = []
        else:
            hits = find_ls_taker_signals(conn, sym_id, ls_info['threshold'])
        last_signal_ts = -SIGNAL_COOLDOWN * POINT_INTERVAL
        sym_had_signals = False

        for ts, ls_ratio, taker_ratio in hits:
            if ts - last_signal_ts < SIGNAL_COOLDOWN * POINT_INTERVAL:
                continue

            oi_idx = oi_ts_to_idx.get(ts)
            if oi_idx is None:
                # Ближайший OI index >= ts
                for idx, (oi_ts, _, _) in enumerate(oi_points):
                    if oi_ts >= ts:
                        oi_idx = idx
                        break
            if oi_idx is None:
                continue

            entry_price = oi_points[oi_idx][2]  # mark_price
            oi_usd = oi_points[oi_idx][1]
            if not entry_price or entry_price <= 0:
                continue

            path, pct_changes = build_price_path(oi_points, oi_idx, entry_price)
            trade = simulate_trade(pct_changes, path,
                                   TAKE_PROFIT, STOP_LOSS, MAX_HOLD_POINTS)
            funding = load_nearest_metric(conn, 'funding_rate', 'rate', sym_id, ts)

            all_signals.append({
                'symbol': sym_name,
                'signal_time': ts,
                'entry_price': entry_price,
                'ls_ratio': ls_ratio,
                'ls_threshold': ls_info['threshold'],
                'taker_ratio': taker_ratio,
                'funding_rate': funding,
                'oi_usd': oi_usd,
                'pct_changes': pct_changes,
                'trade': trade,
            })
            last_signal_ts = ts
            sym_had_signals = True

        if sym_had_signals:
            pairs_with_lt_signals += 1

        # --- OI Flush сигналы (для сравнения) ---
        for sig in find_oi_flush_signals(oi_points):
            idx = sig['point_idx']
            ts_oi, oi_usd, entry_price = oi_points[idx]
            if entry_price <= 0:
                continue
            path, pct_changes = build_price_path(oi_points, idx, entry_price)
            trade = simulate_trade(pct_changes, path,
                                   TAKE_PROFIT, STOP_LOSS, MAX_HOLD_POINTS)
            ls = load_nearest_metric(conn, 'long_short_ratio', 'ratio',
                                     sym_id, ts_oi)
            tk = load_nearest_metric(conn, 'taker_ratio', 'buy_sell_ratio',
                                     sym_id, ts_oi)
            oi_flush_signals.append({
                'symbol': sym_name,
                'signal_time': ts_oi,
                'entry_price': entry_price,
                'ls_ratio': ls,
                'taker_ratio': tk,
                'pct_changes': pct_changes,
                'trade': trade,
            })

    conn.close()
    all_signals.sort(key=lambda s: s['signal_time'])

    # ─── ЗАГОЛОВОК ────────────────────────────────────

    hours = (db_max_ts - db_min_ts) / 3600
    hold_str = f"hold ≤{MAX_HOLD_POINTS * 5}мин" if MAX_HOLD_POINTS > 0 else "hold=∞"

    print()
    print("═══ L/S + TAKER BACKTEST (SHORT) ═══")
    print(f"Период: {ts_to_str(db_min_ts)} — {ts_to_str(db_max_ts)} ({hours:.0f}ч)")
    print(f"Пар с данными L/S: {len(ls_thresholds)} | Пар с сигналами: {pairs_with_lt_signals}")
    print(f"Параметры: L/S > mean+{LS_ZSCORE}σ (мин {LS_MIN_ABS}), "
          f"Taker < {TAKER_THRESHOLD}, TP={TAKE_PROFIT}%, SL={STOP_LOSS}%, {hold_str}")

    # Диагностика адаптивных порогов
    sym_id_to_name = {v: k for k, v in {sym_id: sym_name
                      for sym_id, sym_name in symbols.items()}.items()}
    diag = []
    for sym_id, info in ls_thresholds.items():
        name = symbols.get(sym_id, f"ID:{sym_id}")
        diag.append((name, info))
    diag.sort(key=lambda x: x[1]['threshold'], reverse=True)

    print(f"\nАдаптивные пороги L/S (топ-10 по порогу):")
    for name, info in diag[:10]:
        tag = "adaptive" if info['adaptive'] >= LS_MIN_ABS else "min abs"
        print(f"  {name:15s} mean={info['mean']:.2f} σ={info['stdev']:.2f} "
              f"порог={info['threshold']:.2f} ({tag})")

    if not all_signals:
        print("\nСигналов не найдено.")
        print(f"\nВремя выполнения: {time.time() - start_time:.1f}с")
        return

    # ─── СПИСОК СИГНАЛОВ ──────────────────────────────

    print(f"\n───── СИГНАЛЫ ({len(all_signals)}) ─────\n")
    closed_trades = []
    open_trades = 0

    for i, sig in enumerate(all_signals, 1):
        trade = sig['trade']
        fr_s = (f"{sig['funding_rate'] * 100:+.3f}%"
                if sig['funding_rate'] is not None else "n/a")

        print(f"#{i:<3} {sig['symbol']} | {ts_to_str(sig['signal_time'])} | "
              f"SHORT @ {fmt_price(sig['entry_price'])}")
        print(f"     L/S: {sig['ls_ratio']:.2f} (порог {sig['ls_threshold']:.2f}) "
              f"| Taker: {sig['taker_ratio']:.2f} | Funding: {fr_s}")

        if trade:
            hold_min = trade['hold_points'] * (POINT_INTERVAL // 60)
            print(f"     → {fmt_exit(trade['exit_type'], trade['pnl_pct'], hold_min)} "
                  f"| Выход: {fmt_price(trade['exit_price'])}")
            closed_trades.append({**sig, **trade})
        else:
            print(f"     → ⏳ OPEN (данные закончились)")
            open_trades += 1
        print()

    # ─── СВОДНАЯ СТАТИСТИКА ───────────────────────────

    _print_stats(closed_trades, open_trades, len(all_signals))

    # ─── СРАВНЕНИЕ С OI FLUSH ─────────────────────────

    _print_comparison(closed_trades, oi_flush_signals, hours)

    # ─── ОПТИМИЗАЦИЯ TP/SL ────────────────────────────

    run_optimization(all_signals, hours)

    # ─── ВРЕМЯ ────────────────────────────────────────

    print(f"\nВремя выполнения: {time.time() - start_time:.1f}с")


if __name__ == "__main__":
    main()
