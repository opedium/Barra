"""Live PK calibration: extract PK scores and compare with gift diamonds.

Usage: python scripts/live_pk_check.py <session_dir>
Example: python scripts/live_pk_check.py data/832976560257/20260527_0129_7644204139206937380
"""
import sys, os, csv, json

SCORE_DIVISOR = 10**9  # PK score / 10^9 = 音浪

def read_varint(data, pos):
    value = 0; shift = 0
    while pos < len(data):
        b = data[pos]; pos += 1
        value |= (b & 0x7f) << shift
        if not (b & 0x80): return value, pos
        shift += 7
    return None, pos

def read_ld(data, pos):
    length, pos = read_varint(data, pos)
    if length is None or pos + length > len(data): return None, pos
    return data[pos:pos + length], pos + length

def extract_pk_scores(data):
    """Extract f6 (score_self), f7 (opponent_id), f11 (timestamp) from UpdateScore."""
    result = {}
    pos = 0
    while pos < len(data):
        tag, pos = read_varint(data, pos)
        if tag is None: break
        fn = tag >> 3; wt = tag & 0x7
        if wt == 0:
            val, pos = read_varint(data, pos)
            result[f'f{fn}'] = val
        elif wt == 2:
            raw, pos = read_ld(data, pos)
            if raw is None: continue
            if fn == 10:
                try: result['linker_id'] = raw.decode('utf-8')
                except: pass
        elif wt == 5:
            pos += 4
    return result

def extract_battle_end(data):
    """Extract f3 (start_ts), f4 (duration), f20 (end_ts) from BattleEndPunish."""
    result = {}
    pos = 0
    while pos < len(data):
        tag, pos = read_varint(data, pos)
        if tag is None: break
        fn = tag >> 3; wt = tag & 0x7
        if wt == 0:
            val, pos = read_varint(data, pos)
            result[f'f{fn}'] = val
        elif wt == 2:
            raw, pos = read_ld(data, pos)
            if raw is None: continue
            if fn in (18, 31):
                nested = {}
                np = 0
                while np < len(raw):
                    tag2, np = read_varint(raw, np)
                    if tag2 is None: break
                    fn2 = tag2 >> 3; wt2 = tag2 & 0x7
                    if wt2 == 0:
                        v, np = read_varint(raw, np)
                        nested[f'f{fn2}'] = v
                    elif wt2 == 2:
                        sraw, np = read_ld(raw, np)
                        if sraw is None: continue
                        try:
                            s = sraw.decode('utf-8')
                            if s.isprintable():
                                nested[f'f{fn2}'] = s
                        except: pass
                    elif wt2 == 5:
                        np += 4
                if nested:
                    result[f'f{fn}_nested'] = nested
        elif wt == 5:
            pos += 4
    return result

def main():
    session_dir = sys.argv[1] if len(sys.argv) > 1 else None
    if not session_dir:
        print("Usage: python live_pk_check.py <session_dir>")
        return

    dump_dir = os.path.join(session_dir, 'pk_dumps')
    gift_path = os.path.join(session_dir, 'gift.csv')

    # --- Extract PK scores ---
    print("=" * 90)
    print("PK SCORES (UpdateScore messages)")
    print("=" * 90)
    print(f"{'time':<8} {'score_raw':>22}  {'score_音浪':>12}  {'opponent_id':<20}  linker_id")
    print("-" * 90)

    if not os.path.isdir(dump_dir):
        print("(no pk_dumps yet)")
        return

    score_entries = []
    for f in sorted(os.listdir(dump_dir)):
        if 'UpdateScore' not in f:
            continue
        path = os.path.join(dump_dir, f)
        with open(path, 'rb') as fh:
            data = fh.read()
        fields = extract_pk_scores(data)
        time_str = f.split('_')[0]
        ts = f"{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
        score_raw = fields.get('f6', 0)
        score_diamond = score_raw / SCORE_DIVISOR
        opp_id = fields.get('f7', '?')
        linker = fields.get('linker_id', '')
        score_entries.append((ts, score_raw, score_diamond, opp_id, linker, len(data)))

    for ts, raw, diamond, opp, linker, size in score_entries:
        print(f"{ts:<8} {raw:>22}  {diamond:>12.1f}  {str(opp):<20}  {linker}")

    # --- Extract BattleEndPunish ---
    print()
    print("=" * 90)
    print("PK END EVENTS (BattleEndPunish)")
    print("=" * 90)
    for f in sorted(os.listdir(dump_dir)):
        if 'BattleEndPunish' not in f:
            continue
        path = os.path.join(dump_dir, f)
        with open(path, 'rb') as fh:
            data = fh.read()
        fields = extract_battle_end(data)
        time_str = f.split('_')[0]
        ts = f"{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
        print(f"\n{ts}: duration={fields.get('f4','?')}s, "
              f"start_ts={fields.get('f3','?')}, end_ts={fields.get('f20','?')}")
        nested = fields.get('f18_nested', {})
        if nested:
            print(f"  status: {nested}")
        nested = fields.get('f31_nested', {})
        if nested:
            print(f"  opponent: {nested}")

    # --- Gift summary ---
    print()
    print("=" * 90)
    print("GIFT SUMMARY")
    print("=" * 90)
    if os.path.exists(gift_path):
        total = 0
        count = 0
        with open(gift_path, 'r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                total += int(row['diamond_total'])
                count += 1
        print(f"Total: {count} gifts, {total} 音浪")

        # Per-PK summary if we have scores
        if score_entries:
            # Use the first and last UpdateScore to define PK window
            print()
            print("Per-interval breakdown:")
            prev = None
            for ts, raw, diamond, opp, linker, size in score_entries:
                if prev:
                    p_ts, p_raw, p_diamond, _, _, _ = prev
                    score_delta = raw - p_raw
                    score_delta_d = score_delta / SCORE_DIVISOR
                    gifts = 0
                    diamonds = 0
                    with open(gift_path, 'r', encoding='utf-8-sig') as fh:
                        for row in csv.DictReader(fh):
                            t = row['time']
                            if p_ts <= t <= ts:
                                gifts += 1
                                diamonds += int(row['diamond_total'])
                    ratio = (score_delta / SCORE_DIVISOR) / diamonds if diamonds > 0 else 0
                    print(f"  {p_ts}-{ts}: {gifts:>4} gifts, {diamonds:>8} 音浪, "
                          f"pk_score_delta={score_delta_d:>10.1f}, ratio={ratio:.2f}")
                prev = (ts, raw, diamond, opp, linker, size)
    else:
        print("(no gift.csv yet)")

if __name__ == '__main__':
    main()
