import csv
import os
from collections import defaultdict

os.environ['PYTHONIOENCODING'] = 'utf-8'

FILE = 'G:/DouyinBarrage-main/data/447840496489/20260518_2147_7641227597724437290/gift.csv'

rows = []
with open(FILE, 'r', encoding='utf-8-sig') as f:
    for r in csv.DictReader(f):
        rows.append(r)

# Group by uid+gid
combo_map = defaultdict(list)
for r in rows:
    gid = r['group_id']
    uid = r['user_id']
    if gid and gid != '0':
        combo_map[f'{uid}_{gid}'].append(r)

# How many combos have NO end=1?
no_end1 = 0
has_end1 = 0
end1_only = 0
end0_only = 0

for cid, entries in combo_map.items():
    ends = set(e.get('raw_repeat_end','') for e in entries)
    if '1' in ends:
        has_end1 += 1
    else:
        no_end1 += 1
    if ends == {'1'}:
        end1_only += 1
    if ends == {'0'}:
        end0_only += 1

print(f'Total uid+gid combos: {len(combo_map)}')
print(f'Combos WITH end=1: {has_end1}')
print(f'Combos WITHOUT end=1: {no_end1}')
print(f'Combos with ONLY end=1: {end1_only}')
print(f'Combos with ONLY end=0: {end0_only}')

# For combos without end=1, what's their total diamond value?
lost_sum = 0
lost_combos = []
for cid, entries in combo_map.items():
    ends = set(e.get('raw_repeat_end','') for e in entries)
    if '1' not in ends:
        max_entry = max(entries, key=lambda e: int(e['diamond_total']))
        lost_sum += int(max_entry['diamond_total'])
        if len(lost_combos) < 5:
            lost_combos.append((cid, entries))

print(f'\nLost sum (combos without end=1, max value): {lost_sum}')
print(f'Sample lost combos:')
for cid, entries in lost_combos:
    for e in entries:
        print(f'  {e["time"]} {e["user_name"]} {e["gift_name"]} cnt={e["gift_count"]} diamond={e["diamond_total"]} end={e["raw_repeat_end"]}')

# Check: for combos with end=1, does the end=1 row have the max diamond?
print(f'\n=== END=1 ACCURACY CHECK ===')
wrong_max = 0
for cid, entries in combo_map.items():
    ends = set(e.get('raw_repeat_end','') for e in entries)
    if '1' not in ends:
        continue
    end1_rows = [e for e in entries if e.get('raw_repeat_end') == '1']
    max_diamond = max(int(e['diamond_total']) for e in entries)
    end1_max = max(int(e['diamond_total']) for e in end1_rows)
    if end1_max < max_diamond:
        wrong_max += 1
        if wrong_max <= 3:
            print(f'  {cid}: end1_max={end1_max} < overall_max={max_diamond}')
            for e in entries:
                print(f'    {e["time"]} {e["gift_name"]} cnt={e["gift_count"]} diamond={e["diamond_total"]} end={e["raw_repeat_end"]}')

print(f'Combos where end=1 row is NOT the max: {wrong_max}')

# Check non-increasing combos
print(f'\n=== NON-MONOTONIC COMBOS ===')
non_mono = 0
for cid, entries in combo_map.items():
    if len(entries) < 3:
        continue
    sorted_entries = sorted(entries, key=lambda e: e['time'])
    counts = [int(e['gift_count']) for e in sorted_entries]
    if not all(counts[i] <= counts[i+1] for i in range(len(counts)-1)):
        non_mono += 1
        if non_mono <= 3:
            print(f'\n  {cid}:')
            for e in sorted_entries:
                print(f'    {e["time"]} {e["gift_name"]} cnt={e["gift_count"]} diamond={e["diamond_total"]} end={e["raw_repeat_end"]}')

print(f'Total non-monotonic combos: {non_mono}')
