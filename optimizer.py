"""
Оптимизатор TP/SL для OI Flush бэктеста.
Перебирает все комбинации TP/SL и фильтров, находит оптимальные параметры.
Импортируется из backtest_oi_flush.py
"""

TP_RANGE = [0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
SL_RANGE = [0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]

FILTERS = [
    ("Все сигналы", lambda s: True),
    ("L/S > 2.0", lambda s: s.get('ls_ratio') is not None and s['ls_ratio'] > 2.0),
    ("Taker < 1.0", lambda s: s.get('taker_ratio') is not None and s['taker_ratio'] < 1.0),
    ("L/S > 2.0 + Taker < 1.0", lambda s: (
        s.get('ls_ratio') is not None and s['ls_ratio'] > 2.0
        and s.get('taker_ratio') is not None and s['taker_ratio'] < 1.0)),
]


def _fmt(pnl):
    return f"+{pnl:.2f}%" if pnl >= 0 else f"{pnl:.2f}%"


def simulate_combo(signals, tp, sl):
    """Simulate all signals with given TP/SL. Return stats dict or None."""
    results = []
    for sig in signals:
        pcts = sig['pct_changes']
        if not pcts:
            continue
        pnl = None
        for pct in pcts:
            if pct <= -tp:
                pnl = tp
                break
            elif pct >= sl:
                pnl = -sl
                break
        if pnl is None:
            pnl = -pcts[-1]  # close at last price (SHORT P&L)
        results.append(pnl)

    if not results:
        return None

    trades = len(results)
    wins = sum(1 for r in results if r > 0)
    total_pnl = sum(results)
    gross_profit = sum(r for r in results if r > 0)
    gross_loss = abs(sum(r for r in results if r <= 0))

    return {
        'tp': tp, 'sl': sl,
        'rr': tp / sl if sl > 0 else 999.0,
        'total_pnl': total_pnl,
        'avg_pnl': total_pnl / trades,
        'win_rate': wins / trades * 100,
        'trades': trades,
        'wins': wins,
        'losses': trades - wins,
        'profit_factor': gross_profit / gross_loss if gross_loss > 0 else 999.0,
    }


def optimize_for_signals(signals, min_trades=3):
    """Run all TP/SL combos for a set of signals."""
    combos = []
    for tp in TP_RANGE:
        for sl in SL_RANGE:
            stats = simulate_combo(signals, tp, sl)
            if stats and stats['trades'] >= min_trades:
                combos.append(stats)
    return combos


def print_table(title, combos, top_n=10):
    """Print formatted table of top combos."""
    if not combos:
        print(f"\n{title}: нет данных")
        return
    print(f"\n{title}:")
    print("┌────┬───────┬───────┬──────────┬──────────┬──────────┬────────────────┐")
    print("│  # │  TP%  │  SL%  │ R:R      │ Win rate │ Общий P&L│ Avg на сделку  │")
    print("├────┼───────┼───────┼──────────┼──────────┼──────────┼────────────────┤")
    for i, c in enumerate(combos[:top_n], 1):
        rr_str = f"{c['rr']:.1f}:1"
        print(f"│ {i:2d} │ {c['tp']:5.2f} │ {c['sl']:5.2f} │ "
              f"{rr_str:8s} │ {c['win_rate']:5.0f}%   │ "
              f"{_fmt(c['total_pnl']):8s} │ {_fmt(c['avg_pnl']):14s} │")
    print("└────┴───────┴───────┴──────────┴──────────┴──────────┴────────────────┘")


def print_heatmap(combos):
    """Print text-based P&L heatmap."""
    pnl_map = {(c['tp'], c['sl']): c['total_pnl'] for c in combos}

    tp_show = [0.5, 1.0, 2.0, 3.0, 5.0, 7.0, 10.0]
    sl_show = [0.25, 0.50, 1.00, 1.50, 2.00, 3.00, 5.00, 7.00, 10.00]

    print("\nТепловая карта P&L (все сигналы):")
    header = "         SL:"
    for sl in sl_show:
        header += f" {sl:5.2f}"
    print(header)

    for tp in tp_show:
        row = f"TP {tp:5.2f}:"
        for sl in sl_show:
            val = pnl_map.get((tp, sl))
            if val is not None:
                row += f" {val:+5.1f}"
            else:
                row += "   n/a"
        print(row)
    print("(+) = прибыль, (-) = убыток")


def find_best_configs(all_results, hours):
    """Find and print best configs across all filter sets."""
    best_profit = None
    best_winrate = None
    best_balance = None

    for filter_name, combos in all_results:
        if not combos:
            continue

        # A) Max total P&L
        by_pnl = max(combos, key=lambda c: c['total_pnl'])
        if best_profit is None or by_pnl['total_pnl'] > best_profit[1]['total_pnl']:
            best_profit = (filter_name, by_pnl)

        # B) Max win rate (profitable, min 5 trades)
        eligible = [c for c in combos if c['trades'] >= 5 and c['total_pnl'] > 0]
        if eligible:
            by_wr = max(eligible, key=lambda c: (c['win_rate'], c['total_pnl']))
            if best_winrate is None or by_wr['win_rate'] > best_winrate[1]['win_rate']:
                best_winrate = (filter_name, by_wr)

        # C) Best balance: win_rate>50%, pnl>0, R:R>=1.5, trades>=5
        for criteria in [
            lambda c: c['win_rate'] > 50 and c['total_pnl'] > 0 and c['rr'] >= 1.5 and c['trades'] >= 5,
            lambda c: c['win_rate'] > 50 and c['total_pnl'] > 0 and c['trades'] >= 5,
            lambda c: c['win_rate'] > 50 and c['total_pnl'] > 0 and c['trades'] >= 3,
        ]:
            balanced = [c for c in combos if criteria(c)]
            if balanced:
                by_bal = max(balanced, key=lambda c: c['total_pnl'] * c['win_rate'] / 100)
                if best_balance is None:
                    best_balance = (filter_name, by_bal)
                elif (by_bal['total_pnl'] * by_bal['win_rate'] / 100 >
                      best_balance[1]['total_pnl'] * best_balance[1]['win_rate'] / 100):
                    best_balance = (filter_name, by_bal)
                break  # found with strictest criteria for this filter

    print("\n═══ ЛУЧШИЕ КОНФИГУРАЦИИ ═══")

    def _print_config(emoji, label, entry):
        if not entry:
            return
        fn, c = entry
        print(f"\n{emoji} {label}:")
        print(f"   TP={c['tp']}% SL={c['sl']}% | {fn}")
        print(f"   {c['trades']} сделок | Win rate {c['win_rate']:.0f}% | "
              f"P&L {_fmt(c['total_pnl'])} | Avg {_fmt(c['avg_pnl'])}")

    _print_config("🏆", "МАКС ПРИБЫЛЬ", best_profit)
    _print_config("🎯", "МАКС WIN RATE (прибыльный)", best_winrate)
    _print_config("⚖️ ", "ЛУЧШИЙ БАЛАНС (win rate > 50% И P&L > 0 И R:R >= 1.5)", best_balance)

    print(f"\n⚠️  Данные: {hours:.0f} часов.", end="")
    if hours < 168:
        print(" Рекомендация предварительная.")
        print(f"   Для надёжности нужно 7+ дней (200+ сделок без фильтров).")
    else:
        print()


def run_optimization(signals, hours):
    """Main entry point — called from backtest_oi_flush.py."""
    print("\n\n═══ ОПТИМИЗАЦИЯ TP/SL ═══")

    all_results = []

    for filter_name, filter_fn in FILTERS:
        filtered = [s for s in signals if filter_fn(s)]

        if len(filtered) < 3:
            print(f"\n──── {filter_name} ({len(filtered)} сделок) ──── пропуск (< 3)")
            all_results.append((filter_name, []))
            continue

        is_all = filter_name == "Все сигналы"
        top_n = 10 if is_all else 5
        min_trades_wr = 5 if is_all else 3

        combos = optimize_for_signals(filtered, min_trades=3)
        all_results.append((filter_name, combos))

        if not combos:
            continue

        print(f"\n──── {filter_name} ({len(filtered)} сделок) ────")

        by_pnl = sorted(combos, key=lambda c: c['total_pnl'], reverse=True)
        print_table(f"ТОП-{top_n} по ОБЩЕМУ P&L", by_pnl, top_n)

        eligible = [c for c in combos if c['trades'] >= min_trades_wr]
        by_wr = sorted(eligible, key=lambda c: (c['win_rate'], c['total_pnl']), reverse=True)
        print_table(f"ТОП-{top_n} по WIN RATE (мин {min_trades_wr} сделок)", by_wr, top_n)

        if is_all:
            print_heatmap(combos)

    find_best_configs(all_results, hours)
