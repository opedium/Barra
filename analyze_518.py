import csv
import os
from collections import defaultdict

os.environ['PYTHONIOENCODING'] = 'utf-8'

FILE = 'G:/DouyinBarrage-main/data/447840496489/20260518_2147_7641227597724437290/gift.csv'

rows = []
with open(FILE, 'r', encoding='utf-8-sig') as f:
    for r in csv.DictReader(f):
        rows.append(r)

print(f'Total rows: {len(rows)}')

# Dist of repeat_end
end_counts = defaultdict(int)
for r in rows:
    end_counts[r.get('raw_repeat_end', '?')] += 1
print(f'\nraw_repeat_end distribution: {dict(end_counts)}')

# Dist of raw_group_count values
gc_vals = defaultdict(int)
for r in rows:
    gc_vals[r.get('raw_group_count', '?')] += 1
top_gc = sorted(gc_vals.items(), key=lambda x: -x[1])[:10]
print(f'Top raw_group_count values: {top_gc}')

# Check: how many rows have combo_count != gift_count?
mismatch = [r for r in rows if r['raw_combo_count'] != '' and int(r['raw_combo_count']) != int(r['gift_count'])]
print(f'\nRows where raw_combo_count != gift_count: {len(mismatch)}')
if mismatch:
    for r in mismatch[:5]:
        print(f"  {r['time']} {r['user_name']} {r['gift_name']} combo={r['raw_combo_count']} gift_count={r['gift_count']} total={r['raw_total_count']} repeat={r['raw_repeat_count']} group={r['raw_group_count']} end={r['raw_repeat_end']}")

# Sum analysis
raw_sum = sum(int(r['diamond_total']) for r in rows)
print(f'\n=== SUMS ===')
print(f'Raw sum (no dedup): {raw_sum}')

# OLD: uid+gid max dedup
old = {}
for r in rows:
    gid = r['group_id']
    uid = r['user_id']
    if not gid or gid == '' or gid == '0':
        old[f"single_{r['time']}_{r['user_name']}_{r['gift_name']}"] = r
    else:
        cid = f'{uid}_{gid}'
        if cid not in old:
            old[cid] = r
        elif int(r['diamond_total']) > int(old[cid]['diamond_total']):
            old[cid] = r
print(f'OLD dedup (uid+gid max): {sum(int(r["diamond_total"]) for r in old.values())} entries={len(old)}')

# NEW: uid+gid+gift max dedup (no repeat_end filter)
new_no_filter = {}
for r in rows:
    gid = r['group_id']
    uid = r['user_id']
    gift = r['gift_name']
    if not gid or gid == '' or gid == '0':
        new_no_filter[f"single_{r['time']}_{r['user_name']}_{gift}"] = r
    else:
        cid = f'{uid}_{gid}_{gift}'
        if cid not in new_no_filter:
            new_no_filter[cid] = r
        elif int(r['diamond_total']) > int(new_no_filter[cid]['diamond_total']):
            new_no_filter[cid] = r
print(f'NEW dedup (uid+gid+gift max, no end filter): {sum(int(r["diamond_total"]) for r in new_no_filter.values())} entries={len(new_no_filter)}')

# NEW: uid+gid+gift max dedup WITH repeat_end=1 filter
new_end1 = {}
for r in rows:
    gid = r['group_id']
    uid = r['user_id']
    gift = r['gift_name']
    end = r.get('raw_repeat_end', '')

    if not gid or gid == '' or gid == '0':
        new_end1[f"single_{r['time']}_{r['user_name']}_{gift}"] = r
        continue

    # Only take end=1 rows
    if end == '' or int(end) != 1:
        continue

    cid = f'{uid}_{gid}_{gift}'
    if cid not in new_end1:
        new_end1[cid] = r
    elif int(r['diamond_total']) > int(new_end1[cid]['diamond_total']):
        new_end1[cid] = r
print(f'NEW dedup (uid+gid+gift max, end=1 only): {sum(int(r["diamond_total"]) for r in new_end1.values())} entries={len(new_end1)}')

# Check: what do combo sequences look like with end=1?
# Find all entries for a specific combo to see the progression
print(f'\n=== COMBO PATTERNS WITH RAW FIELDS ===')
combo_map = defaultdict(list)
for r in rows:
    gid = r['group_id']
    uid = r['user_id']
    if gid and gid != '0':
        combo_map[f'{uid}_{gid}'].append(r)

# Find combos with multiple rows where some have end=1
end1_combos = []
for cid, entries in combo_map.items():
    ends = set(e.get('raw_repeat_end','') for e in entries)
    if len(entries) >= 3 and '1' in ends:
        end1_combos.append((cid, entries))

print(f'Combos with >=3 rows and end=1: {len(end1_combos)}')
for cid, entries in end1_combos[:3]:
    print(f'\n  Combo: {cid}')
    for e in entries:
        print(f'    {e["time"]} {e["gift_name"]} cnt={e["gift_count"]} diamond={e["diamond_total"]} combo={e["raw_combo_count"]} total={e["raw_total_count"]} repeat={e["raw_repeat_count"]} group={e["raw_group_count"]} end={e["raw_repeat_end"]}')

# Check: maybe end=0 vs end=1 distinction shows PROGRESSIVE vs FINAL
# In test: progressive rows had end=0, final had end=1
# In live: is this the same?
# Let's check combos with BOTH end=0 and end=1
mixed_end_combos = []
for cid, entries in combo_map.items():
    ends = set(e.get('raw_repeat_end','') for e in entries)
    if '0' in ends and '1' in ends and len(entries) >= 2:
        mixed_end_combos.append((cid, entries))

print(f'\n\nCombos with BOTH end=0 and end=1: {len(mixed_end_combos)}')
for cid, entries in mixed_end_combos[:5]:
    print(f'\n  Combo: {cid}')
    sorted_entries = sorted(entries, key=lambda e: e['time'])
    for e in sorted_entries:
        print(f'    {e["time"]} {e["gift_name"]} cnt={e["gift_count"]} diamond={e["diamond_total"]} end={e["raw_repeat_end"]}')
