#!/usr/bin/env python3
"""
OI Flush Backtest — SHORT strategy.

Ищет паттерн "OI накопление → сброс" в market_data.db
и симулирует SHORT вход в момент обнаружения сброса.
После основного бэктеста запускает оптимизацию TP/SL.

Запуск: python3 backtest_oi_flush.py
"""

import io
import json
import os
import sqlite3
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from collections import defaultdict

from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ADMIN_ID = os.getenv("ADMIN_ID", "")

# ═══════════════════════════════════════════════════════
# НАСТРАИВАЕМЫЕ ПАРАМЕТРЫ
# ═══════════════════════════════════════════════════════

DB_PATH = os.getenv("DB_PATH", "market_data.db")

# Поиск сигнала
BUILDUP_THRESHOLD = 3.0     # мин % роста OI для "накопления"
BUILDUP_MIN_POINTS = 12     # мин точек подряд выше порога (1 час)
FLUSH_DROP_PCT = 2.0        # мин % падения от пика серии
FLUSH_CURRENT_MAX = 2.0     # текущий pct_change должен быть ниже этого
WINDOW_SIZE = 24            # размер окна в точках (2 часа)
MIN_HISTORY = 24            # мин точек для анализа пары

# Дедупликация
SIGNAL_COOLDOWN = 6         # точек между сигналами одной пары (30 мин)

# Симуляция сделки
TAKE_PROFIT = 3.0           # % прибыли (цена упала)
STOP_LOSS = 1.5             # % убытка (цена выросла)
MAX_HOLD_POINTS = 0         # 0 = без лимита, держать до TP/SL/конца данных

# Интервал между точками (секунд)
POINT_INTERVAL = 300        # 5 минут


# ═══════════════════════════════════════════════════════
# ЗАГРУЗКА ДАННЫХ
# ═══════════════════════════════════════════════════════

def load_symbols(conn):
    cur = conn.execute("SELECT id, symbol FROM symbols WHERE status='active'")
    return {row[0]: row[1] for row in cur.fetchall()}


def load_oi_data(conn, symbol_id):
    cur = conn.execute(
        """SELECT timestamp, oi_usd, mark_price FROM open_interest
           WHERE symbol_id = ? AND oi_usd IS NOT NULL AND mark_price IS NOT NULL
           ORDER BY timestamp ASC""",
        (symbol_id,),
    )
    return cur.fetchall()


def load_nearest_metric(conn, table, column, symbol_id, ts):
    cur = conn.execute(
        f"""SELECT {column} FROM {table}
            WHERE symbol_id = ? AND timestamp <= ?
            ORDER BY timestamp DESC LIMIT 1""",
        (symbol_id, ts),
    )
    row = cur.fetchone()
    return row[0] if row else None


# ═══════════════════════════════════════════════════════
# ПОИСК СИГНАЛОВ
# ═══════════════════════════════════════════════════════

def find_signals(oi_points):
    signals = []
    last_signal_idx = -SIGNAL_COOLDOWN

    for i in range(WINDOW_SIZE, len(oi_points)):
        if i - last_signal_idx < SIGNAL_COOLDOWN:
            continue

        window = oi_points[i - WINDOW_SIZE: i + 1]
        base_oi = window[0][1]
        if base_oi <= 0:
            continue

        pct_changes = [(oi - base_oi) / base_oi * 100 for _, oi, _ in window]
        current_pct = pct_changes[-1]

        if current_pct >= FLUSH_CURRENT_MAX:
            continue

        # Найти лучшую непрерывную серию >= BUILDUP_THRESHOLD
        best_len, best_peak = 0, 0
        run_start, run_len, run_peak = None, 0, 0

        for j, pct in enumerate(pct_changes[:-1]):
            if pct >= BUILDUP_THRESHOLD:
                if run_start is None:
                    run_start, run_len, run_peak = j, 1, pct
                else:
                    run_len += 1
                    run_peak = max(run_peak, pct)
                if run_len > best_len:
                    best_len, best_peak = run_len, run_peak
            else:
                run_start, run_len, run_peak = None, 0, 0

        if best_len < BUILDUP_MIN_POINTS:
            continue
        if best_peak - current_pct < FLUSH_DROP_PCT:
            continue

        signals.append({
            'point_idx': i,
            'oi_peak_pct': best_peak,
            'oi_current_pct': current_pct,
            'oi_buildup_duration': best_len * (POINT_INTERVAL // 60),
        })
        last_signal_idx = i

    return signals


# ═══════════════════════════════════════════════════════
# ЦЕНОВОЙ ПУТЬ И СИМУЛЯЦИЯ
# ═══════════════════════════════════════════════════════

def build_price_path(oi_points, signal_idx, entry_price):
    """Построить массив (timestamp, price, pct_change) после входа."""
    path = []
    pct_changes = []
    for ts, _, price in oi_points[signal_idx + 1:]:
        if price is not None and price > 0:
            pct = (price - entry_price) / entry_price * 100
            path.append((ts, price))
            pct_changes.append(pct)
    return path, pct_changes


def simulate_trade(pct_changes, path, tp, sl, max_hold):
    """Симулировать SHORT из предрассчитанного пути цен."""
    for k, pct in enumerate(pct_changes):
        pts = k + 1
        if pct <= -tp:
            return {'exit_type': 'TP', 'exit_price': path[k][1],
                    'exit_time': path[k][0], 'pnl_pct': tp, 'hold_points': pts}
        elif pct >= sl:
            return {'exit_type': 'SL', 'exit_price': path[k][1],
                    'exit_time': path[k][0], 'pnl_pct': -sl, 'hold_points': pts}
        elif max_hold > 0 and pts >= max_hold:
            return {'exit_type': 'TIMEOUT', 'exit_price': path[k][1],
                    'exit_time': path[k][0], 'pnl_pct': -pct, 'hold_points': pts}
    return None


# ═══════════════════════════════════════════════════════
# ФОРМАТИРОВАНИЕ
# ═══════════════════════════════════════════════════════

def ts_to_str(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')

def fmt_pnl(pnl):
    return f"+{pnl:.2f}%" if pnl >= 0 else f"{pnl:.2f}%"

def fmt_price(p):
    return f"${p:.2f}" if p >= 1000 else f"${p:.4f}" if p >= 1 else f"${p:.6f}"

def fmt_exit(exit_type, pnl, hold_min):
    marker = {'TP': '✅ TP', 'SL': '❌ SL', 'TIMEOUT': '⏱ TIMEOUT'}
    return f"{marker.get(exit_type, exit_type)} {fmt_pnl(pnl)} за {hold_min} мин"


# ═══════════════════════════════════════════════════════
# ВЫВОД В КОНСОЛЬ + ФАЙЛ + TELEGRAM
# ═══════════════════════════════════════════════════════

class Tee:
    """Дублирует вывод в несколько потоков."""
    def __init__(self, *streams):
        self.streams = streams
    def write(self, data):
        for s in self.streams:
            s.write(data)
    def flush(self):
        for s in self.streams:
            s.flush()


def send_to_telegram(file_path):
    """Отправить файл в Telegram через Bot API."""
    if not TELEGRAM_BOT_TOKEN or not ADMIN_ID:
        print("⚠️  TELEGRAM_BOT_TOKEN или ADMIN_ID не заданы, файл не отправлен")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    boundary = "----BacktestBoundary"
    filename = os.path.basename(file_path)
    with open(file_path, "rb") as f:
        file_data = f.read()
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="chat_id"\r\n\r\n'
        f"{ADMIN_ID}\r\n"
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="document"; filename="{filename}"\r\n'
        f"Content-Type: text/plain\r\n\r\n"
    ).encode() + file_data + f"\r\n--{boundary}--\r\n".encode()
    req = urllib.request.Request(
        url, data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read())
            if result.get("ok"):
                print(f"📤 Отправлено в Telegram: {filename}")
            else:
                print(f"⚠️  Telegram ответил ошибкой: {result}")
    except (urllib.error.URLError, OSError) as e:
        print(f"⚠️  Не удалось отправить в Telegram: {e}")


# ═══════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════

def _save_and_send(buf, old_stdout):
    """Восстановить stdout, сохранить результат в .txt, отправить в TG."""
    sys.stdout = old_stdout
    result_text = buf.getvalue()
    if not result_text.strip():
        return
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M")
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            f"backtest_oi_flush_{date_str}.txt")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(result_text)
    print(f"💾 Сохранено: {out_path}")
    send_to_telegram(out_path)


def main():
    start_time = time.time()

    # Захват вывода в буфер для сохранения в файл
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

    # ─── СБОР СИГНАЛОВ С ЦЕНОВЫМИ ПУТЯМИ ─────────────

    all_signals = []
    pairs_with_data = 0

    for sym_id, sym_name in symbols.items():
        oi_points = load_oi_data(conn, sym_id)
        if len(oi_points) < MIN_HISTORY:
            continue
        pairs_with_data += 1

        for sig in find_signals(oi_points):
            idx = sig['point_idx']
            ts, oi_usd, entry_price = oi_points[idx]
            if entry_price <= 0:
                continue

            path, pct_changes = build_price_path(oi_points, idx, entry_price)
            trade = simulate_trade(pct_changes, path, TAKE_PROFIT, STOP_LOSS, MAX_HOLD_POINTS)

            all_signals.append({
                'symbol': sym_name,
                'signal_time': ts,
                'entry_price': entry_price,
                'oi_peak_pct': sig['oi_peak_pct'],
                'oi_current_pct': sig['oi_current_pct'],
                'oi_buildup_duration': sig['oi_buildup_duration'],
                'oi_usd': oi_usd,
                'funding_rate': load_nearest_metric(conn, 'funding_rate', 'rate', sym_id, ts),
                'ls_ratio': load_nearest_metric(conn, 'long_short_ratio', 'ratio', sym_id, ts),
                'taker_ratio': load_nearest_metric(conn, 'taker_ratio', 'buy_sell_ratio', sym_id, ts),
                'pct_changes': pct_changes,
                'trade': trade,
            })

    conn.close()
    all_signals.sort(key=lambda s: s['signal_time'])

    # ─── ЗАГОЛОВОК ────────────────────────────────────

    hours = (db_max_ts - db_min_ts) / 3600
    hold_str = f"hold ≤{MAX_HOLD_POINTS * 5}мин" if MAX_HOLD_POINTS > 0 else "hold=∞"

    print()
    print("═══ OI FLUSH BACKTEST ═══")
    print(f"Период: {ts_to_str(db_min_ts)} — {ts_to_str(db_max_ts)} ({hours:.0f}ч)")
    print(f"Пар с данными: {pairs_with_data} | Мин история: {MIN_HISTORY} точек ({MIN_HISTORY * 5} мин)")
    print(f"Параметры: buildup ≥{BUILDUP_THRESHOLD}% × {BUILDUP_MIN_POINTS * 5}мин, "
          f"drop ≥{FLUSH_DROP_PCT}%, TP={TAKE_PROFIT}%, SL={STOP_LOSS}%, {hold_str}")

    if not all_signals:
        print("\nСигналов не найдено.")
        print(f"\nВремя выполнения: {time.time() - start_time:.1f}с")
        return

    # ─── СПИСОК СИГНАЛОВ ──────────────────────────────

    print("\n───── СИГНАЛЫ ─────\n")
    closed_trades = []
    open_trades = 0

    for i, sig in enumerate(all_signals, 1):
        trade = sig['trade']
        fr_s = f"{sig['funding_rate'] * 100:+.3f}%" if sig['funding_rate'] is not None else "n/a"
        ls_s = f"{sig['ls_ratio']:.2f}" if sig['ls_ratio'] is not None else "n/a"
        tk_s = f"{sig['taker_ratio']:.2f}" if sig['taker_ratio'] is not None else "n/a"

        print(f"#{i:<3} {sig['symbol']} | {ts_to_str(sig['signal_time'])} | "
              f"SHORT @ {fmt_price(sig['entry_price'])}")
        print(f"     OI: пик +{sig['oi_peak_pct']:.1f}% ({sig['oi_buildup_duration']} мин) "
              f"→ сейчас {sig['oi_current_pct']:+.1f}%")
        print(f"     Funding: {fr_s} | L/S: {ls_s} | Taker: {tk_s}")

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

    print("───── ИТОГИ ─────\n")
    print(f"Всего сигналов: {len(all_signals)}")
    print(f"Закрытых сделок: {len(closed_trades)} ({open_trades} ещё открыты)")

    if not closed_trades:
        print(f"\nВремя выполнения: {time.time() - start_time:.1f}с")
        return

    exit_counts = defaultdict(int)
    for t in closed_trades:
        exit_counts[t['exit_type']] += 1

    print("\nПо выходам:")
    for etype in ['TP', 'SL', 'TIMEOUT']:
        cnt = exit_counts.get(etype, 0)
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

    # ─── ФИЛЬТРЫ ──────────────────────────────────────

    filter_defs = [
        ("Funding > 0.01%:", lambda t: t.get('funding_rate') is not None and t['funding_rate'] > 0.0001),
        ("Funding <= 0.01%:", lambda t: t.get('funding_rate') is not None and t['funding_rate'] <= 0.0001),
        ("L/S > 2.0:", lambda t: t.get('ls_ratio') is not None and t['ls_ratio'] > 2.0),
        ("L/S <= 2.0:", lambda t: t.get('ls_ratio') is not None and t['ls_ratio'] <= 2.0),
        ("Taker < 1.0:", lambda t: t.get('taker_ratio') is not None and t['taker_ratio'] < 1.0),
        ("Taker >= 1.0:", lambda t: t.get('taker_ratio') is not None and t['taker_ratio'] >= 1.0),
    ]
    print("\nПо фильтрам (какие сигналы прибыльнее):")
    for label, fn in filter_defs:
        subset = [t for t in closed_trades if fn(t)]
        if subset:
            w = sum(1 for t in subset if t['pnl_pct'] > 0)
            print(f"  {label:25s} win rate {w / len(subset) * 100:5.0f}% ({len(subset)} сделок)")

    # ─── ТОП ПАРЫ ─────────────────────────────────────

    pair_stats = defaultdict(lambda: {'count': 0, 'wins': 0})
    for t in closed_trades:
        pair_stats[t['symbol']]['count'] += 1
        if t['pnl_pct'] > 0:
            pair_stats[t['symbol']]['wins'] += 1

    top_pairs = sorted(pair_stats.items(), key=lambda x: x[1]['count'], reverse=True)
    print("\nТОП пары по кол-ву сигналов:")
    for sym, st in top_pairs[:10]:
        wr = st['wins'] / st['count'] * 100 if st['count'] > 0 else 0
        print(f"  {sym:15s} {st['count']:2d} сигналов, win rate {wr:.0f}%")

    # ─── РЕКОМЕНДАЦИЯ ─────────────────────────────────

    combo_filters = [
        ("OI flush (без фильтров)", lambda t: True),
        ("OI flush + Funding > 0.01%",
         lambda t: t.get('funding_rate') is not None and t['funding_rate'] > 0.0001),
        ("OI flush + L/S > 2.0",
         lambda t: t.get('ls_ratio') is not None and t['ls_ratio'] > 2.0),
        ("OI flush + Taker < 1.0",
         lambda t: t.get('taker_ratio') is not None and t['taker_ratio'] < 1.0),
        ("OI flush + L/S > 2.0 + Taker < 1.0",
         lambda t: (t.get('ls_ratio') is not None and t['ls_ratio'] > 2.0
                    and t.get('taker_ratio') is not None and t['taker_ratio'] < 1.0)),
    ]
    filter_combos = []
    for label, fn in combo_filters:
        subset = [t for t in closed_trades if fn(t)]
        if len(subset) >= 2:
            w = sum(1 for t in subset if t['pnl_pct'] > 0)
            avg = sum(t['pnl_pct'] for t in subset) / len(subset)
            filter_combos.append((label, len(subset), w / len(subset) * 100, avg))

    if filter_combos:
        bc = max(filter_combos, key=lambda x: (x[2], x[3]))
        print(f"\n───── РЕКОМЕНДАЦИЯ ─────\n")
        print(f"Лучшая комбинация фильтров:")
        print(f"  {bc[0]}")
        print(f"  Сделок: {bc[1]} | Win rate: {bc[2]:.0f}% | Avg P&L: {fmt_pnl(bc[3])}")
        if hours < 168:
            print(f"  ⚠️  Мало данных ({hours:.0f}ч) — нужна неделя для статистики")

    # ─── ОПТИМИЗАЦИЯ TP/SL ────────────────────────────

    from optimizer import run_optimization
    run_optimization(all_signals, hours)

    # ─── ВРЕМЯ ────────────────────────────────────────

    print(f"\nВремя выполнения: {time.time() - start_time:.1f}с")


if __name__ == "__main__":
    main()
