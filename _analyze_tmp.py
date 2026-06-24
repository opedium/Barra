import csv, collections, os, json, sys
sys.stdout.reconfigure(encoding='utf-8')

data_dir = r'G:\DouyinBarrage-main\data\447840496489\20260525_2138_7643823097967102739'
gift_path = os.path.join(data_dir, 'gift.csv')

with open(gift_path, encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

print(f'Total gift rows: {len(rows)}')
print(f'Unique users: {len(set(r["user_id"] for r in rows))}')
print()

user_diamond = collections.defaultdict(int)
user_gifts = collections.defaultdict(int)
user_info = {}
for r in rows:
    uid = r['user_id']
    d = int(r['diamond_total']) if r['diamond_total'] else 0
    c = int(r['gift_count']) if r['gift_count'] else 0
    user_diamond[uid] += d
    user_gifts[uid] += c
    user_info[uid] = (r['user_name'], r['douyin_id'])

top = sorted(user_diamond.items(), key=lambda x: x[1], reverse=True)[:15]
print('=== Top 15 by diamond ===')
for i, (uid, d) in enumerate(top, 1):
    name, dyid = user_info[uid]
    cnt = user_gifts[uid]
    print(f'{i:2}. {name} | dyid={dyid} | uid={uid}')
    print(f'    diamond={d:,}  sound={d/10:,.0f}  gifts={cnt}')
    print()

total = sum(user_diamond.values())
print(f'Total diamond: {total:,.0f} = {total/10:,.0f} sound')
print(f'Total gifts: {sum(user_gifts.values()):,}')

# Check dedup_stats
dedup_path = os.path.join(data_dir, 'dedup_stats.csv')
print('\n=== Dedup Stats ===')
with open(dedup_path, encoding='utf-8-sig') as f:
    lines = f.readlines()
    print(f'Snapshots: {len(lines)-1}')
    # first and last 3
    print('First 3:')
    for l in lines[1:4]:
        print(f'  {l.strip()}')
    print('Last 3:')
    for l in lines[-3:]:
        print(f'  {l.strip()}')
