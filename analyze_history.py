import csv
import os
import sys
from collections import defaultdict

os.environ['PYTHONIOENCODING'] = 'utf-8'

def analyze_gift(filepath, label):
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"  {filepath}")
    print(f"{'='*60}")

    rows = []
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)

    if not rows:
        print("  EMPTY FILE")
        return

    raw_sum = sum(int(r['diamond_total']) for r in rows)

    # Check if group_id column exists
    has_group_id = 'group_id' in rows[0] if rows else False
    print(f"  Has group_id column: {has_group_id}")

    # Dedup by user_id + group_id
    dedup = {}
    no_gid_count = 0
    for r in rows:
        if has_group_id:
            gid = r.get('group_id', '')
            if not gid or gid == '' or gid == '0':
                no_gid_count += 1
                tid = f"single_{r['time']}_{r['user_name']}_{r['gift_name']}"
                dedup[tid] = r
            else:
                cid = f"{r['user_id']}_{gid}"
                if cid not in dedup:
                    dedup[cid] = r
                else:
                    cur = int(r['diamond_total'])
                    sto = int(dedup[cid]['diamond_total'])
                    if cur > sto:
                        dedup[cid] = r
        else:
            # Old data without group_id - just unique by time+user+gift+count
            tid = f"{r['time']}_{r['user_id']}_{r['gift_name']}_{r['gift_count']}"
            dedup[tid] = r

    dedup_sum = sum(int(r['diamond_total']) for r in dedup.values())

    # Count combos (need group_id)
    non_inc_combos = 0
    multi_gift_combos = 0
    if has_group_id:
        combo_map = defaultdict(list)
        for r in rows:
            gid = r.get('group_id', '')
            uid = r.get('user_id', '')
            if gid and gid != '0':
                combo_map[f'{uid}_{gid}'].append(r)

        for cid, entries in combo_map.items():
            gifts = set(e['gift_name'] for e in entries)
            if len(gifts) > 1:
                multi_gift_combos += 1
            if len(entries) >= 2:
                counts = [int(e['gift_count']) for e in entries]
                if not all(counts[i] <= counts[i+1] for i in range(len(counts)-1)):
                    non_inc_combos += 1

    print(f"  Total rows: {len(rows)}")
    print(f"  Raw sum: {raw_sum}")
    print(f"  Dedup entries: {len(dedup)}")
    print(f"  Dedup sum: {dedup_sum}")
    print(f"  Difference: {raw_sum - dedup_sum} ({(raw_sum - dedup_sum)/raw_sum*100:.1f}%)")
    print(f"  Rows without group_id: {no_gid_count}")
    print(f"  Non-increasing combos: {non_inc_combos}")
    print(f"  Multi-gift combos: {multi_gift_combos}")

    return raw_sum, dedup_sum

# Analyze key dates
DATA = 'G:/Data_Source'

print("HISTORICAL DATA ANALYSIS")
print("="*60)

# 5.6 = ~380w (reference)
analyze_gift(f'{DATA}/20260506_2204_7636779018305604371_gift.csv', '5.6 (~380w reference)')
# 5.9 = ~523w (reference)
analyze_gift(f'{DATA}/20260509_2137_7637885411519499050_gift.csv', '5.9 (~523w reference)')
# 5.7 and 5.8 (reportedly close to accurate)
analyze_gift(f'{DATA}/20260507_2205_7637150208031230774_gift.csv', '5.7 (reportedly accurate)')
analyze_gift(f'{DATA}/20260508_2206_7637521892571728676_gift.csv', '5.8 (reportedly accurate)')
# Also check 5.5 and 5.13 which have _dedup files
analyze_gift(f'{DATA}/20260505_2215_7636408661497088810_gift.csv', '5.5')
analyze_gift(f'{DATA}/20260505_2215_7636408661497088810_gift_dedup.csv', '5.5 DEDUP')
analyze_gift(f'{DATA}/20260513_2136_7639369489213115176_gift.csv', '5.13')
analyze_gift(f'{DATA}/20260513_2136_7639369489213115176_gift_dedup.csv', '5.13 DEDUP')
