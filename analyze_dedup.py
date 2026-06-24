import csv
import sys
import os
from collections import defaultdict

# Force utf-8 output on Windows
os.environ['PYTHONIOENCODING'] = 'utf-8'

GIFT_FILE = 'G:/DouyinBarrage-main/data/447840496489/20260517_2145_7640856207502183231/gift.csv'
CLEAN_FILE = 'G:/DouyinBarrage-main/data/447840496489/20260517_2145_7640856207502183231/gift_cleaned.csv'

rows = []
with open(GIFT_FILE, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for r in reader:
        rows.append(r)

print(f'=== BASIC STATS ===')
print(f'Total rows: {len(rows)}')

# Combos
combos = [r for r in rows if int(r['gift_count']) > 1]
print(f'Rows with gift_count > 1: {len(combos)}')

if combos:
    print(f'\nSample combos (first 15):')
    for c in combos[:15]:
        print(f"  {c['time']} | {c['user_name'][:12]} | {c['gift_name']} x{c['gift_count']} | diamond={c['diamond_total']} | gid={c['group_id']}")

# Unique values
all_gids = set(r['group_id'] for r in rows)
print(f'\nUnique group_ids: {len(all_gids)}')

# Group by user_id + group_id
combo_map = defaultdict(list)
for r in rows:
    gid = r['group_id']
    uid = r['user_id']
    if gid and gid != '0':
        combo_map[f'{uid}_{gid}'].append(r)

# Check for combos with multiple gift types (same user, same gid, different gift)
multi_gift = 0
for cid, entries in combo_map.items():
    gifts = set(e['gift_name'] for e in entries)
    if len(gifts) > 1:
        multi_gift += 1
        if multi_gift <= 5:
            print(f'\nMULTI-GIFT COMBO: {cid}')
            for e in entries:
                print(f"  {e['time']} | {e['user_name']} | {e['gift_name']} x{e['gift_count']} | gid={e['group_id']}")

print(f'\nTotal combo_ids with multiple gift types: {multi_gift}')

# Check for gids shared across users
gid_users = defaultdict(set)
for r in rows:
    gid = r['group_id']
    uid = r['user_id']
    if gid and gid != '0':
        gid_users[gid].add(uid)

multi_user_gids = {gid: users for gid, users in gid_users.items() if len(users) > 1}
print(f'Group_ids shared across multiple users: {len(multi_user_gids)}')
if multi_user_gids:
    # Show worst offenders
    worst = sorted(multi_user_gids.items(), key=lambda x: len(x[1]), reverse=True)[:5]
    for gid, users in worst:
        print(f"  gid={gid}: {len(users)} users")

print(f'\n=== SUMS ===')
raw_sum = sum(int(r['diamond_total']) for r in rows)
print(f'Raw sum (no dedup): {raw_sum}')

# Dedup by user_id + group_id
dedup = {}
for r in rows:
    gid = r['group_id']
    uid = r['user_id']
    if not gid or gid == '' or gid == '0':
        tid = f"single_{r['time']}_{r['user_name']}_{r['gift_name']}"
        dedup[tid] = r
    else:
        cid = f'{uid}_{gid}'
        if cid not in dedup:
            dedup[cid] = r
        else:
            cur = int(r['diamond_total'])
            sto = int(dedup[cid]['diamond_total'])
            if cur > sto:
                dedup[cid] = r

dedup_sum = sum(int(r['diamond_total']) for r in dedup.values())
print(f'Dedup (uid+gid, max diamond): {dedup_sum} (entries: {len(dedup)})')

# Check cleaned file
clean_rows = []
with open(CLEAN_FILE, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for r in reader:
        clean_rows.append(r)
clean_sum = sum(int(r['diamond_total']) for r in clean_rows)
print(f'Cleaned file sum: {clean_sum} (entries: {len(clean_rows)})')

# --- NEW: Check for the "interleaved combo" problem ---
# Scenario: same user, same gift, multiple group_ids = multiple separate combos
# This should be fine. But if same user, same gift, SAME group_id
# but different combo sequences, that's a problem.
# Let's check: for each user+gift+group_id, look at the count progression
print(f'\n=== COMBO PROGRESSION ANALYSIS ===')
user_gift_gid = defaultdict(list)
for r in rows:
    key = (r['user_id'], r['gift_name'], r['group_id'])
    user_gift_gid[key].append(r)

# Find combos where count doesn't monotonically increase
broken = 0
for key, entries in user_gift_gid.items():
    if len(entries) < 2:
        continue
    counts = [int(e['gift_count']) for e in entries]
    # Check if strictly increasing (combo progression)
    is_increasing = all(counts[i] <= counts[i+1] for i in range(len(counts)-1))
    if not is_increasing:
        broken += 1
        if broken <= 5:
            uid, gift, gid = key
            print(f'  Non-increasing combo: uid={uid[:20]} gift={gift} gid={gid}')
            for e in entries:
                print(f'    {e["time"]} count={e["gift_count"]} diamond={e["diamond_total"]}')

print(f'Total non-increasing combos: {broken}')

# Now check: for same user + same gift, do group_ids overlap?
# i.e. can a user have multiple active combos of the SAME gift?
print(f'\n=== SAME USER, SAME GIFT, MULTIPLE GROUP_IDS ===')
user_gift_gids = defaultdict(set)
for r in rows:
    key = (r['user_id'], r['gift_name'])
    user_gift_gids[key].add(r['group_id'])

multi_gid_gifts = {k: v for k, v in user_gift_gids.items() if len(v) > 1}
print(f'User-gift pairs with multiple group_ids: {len(multi_gid_gifts)}')
if multi_gid_gifts:
    for (uid, gift), gids in list(multi_gid_gifts.items())[:5]:
        print(f'  uid={uid[:20]} gift={gift}: {len(gids)} gids')
