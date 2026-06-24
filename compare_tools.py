#!/usr/bin/python
# coding:utf-8
"""
双工具对比分析
用法: python compare_tools.py <DouyinBarrage_gift.csv> <DouyinLiveWebFetcher_gift.csv>

对比维度:
1. 总行数差异
2. 相同时间段的消息数对比
3. 钻石总和差异（原始 + 去重）
4. 独占消息（只有一个工具捕获到的）
"""

import sys
import csv
from collections import defaultdict

def load_csv(path):
    rows = []
    with open(path, 'r', encoding='utf-8-sig') as f:
        for r in csv.DictReader(f):
            rows.append(r)
    return rows

def main(file_a, file_b):
    print("=" * 60)
    print("  双工具对比分析")
    print("=" * 60)
    print(f"A (DouyinBarrage): {file_a}")
    print(f"B (替代工具):       {file_b}")
    print()

    rows_a = load_csv(file_a)
    rows_b = load_csv(file_b)

    print(f"=== 基本统计 ===")
    print(f"A 总行数: {len(rows_a):,}")
    print(f"B 总行数: {len(rows_b):,}")
    print(f"差异: {len(rows_b) - len(rows_a):,} (B - A)")
    if len(rows_a) > 0:
        print(f"比例: {len(rows_b)/len(rows_a)*100:.1f}%")

    # 时间范围
    times_a = [r['time'] for r in rows_a]
    times_b = [r['time'] for r in rows_b]
    print(f"\nA 时间范围: {min(times_a)} - {max(times_a)}" if times_a else "A: 无数据")
    print(f"B 时间范围: {min(times_b)} - {max(times_b)}" if times_b else "B: 无数据")

    # 钻石对比
    raw_a = sum(int(r['diamond_total']) for r in rows_a)
    raw_b = sum(int(r['diamond_total']) for r in rows_b)
    print(f"\n=== 钻石原始求和 ===")
    print(f"A: {raw_a:,}")
    print(f"B: {raw_b:,}")
    print(f"差异: {raw_b - raw_a:,} ({((raw_b-raw_a)/raw_a*100):.1f}%)" if raw_a else "")

    # 去重对比 (uid+gid+gift max)
    def dedup(rows):
        m = {}
        for r in rows:
            gid = r.get('group_id','')
            uid = r.get('user_id','')
            gift = r.get('gift_name','')
            if not gid or gid == '0':
                key = f"single_{r['time']}_{uid}_{gift}"
            else:
                key = f"{uid}_{gid}_{gift}"
            if key not in m or int(r['diamond_total']) > int(m[key]['diamond_total']):
                m[key] = r
        return sum(int(v['diamond_total']) for v in m.values()), len(m)

    dedup_a, combo_a = dedup(rows_a)
    dedup_b, combo_b = dedup(rows_b)
    print(f"\n=== 去重后钻石 (uid+gid+gift max) ===")
    print(f"A: {dedup_a:,} ({combo_a:,} combos)")
    print(f"B: {dedup_b:,} ({combo_b:,} combos)")
    if dedup_a:
        print(f"差异: {dedup_b - dedup_a:,} ({((dedup_b-dedup_a)/dedup_a*100):.1f}%)")

    # 分钟级消息密度对比
    print(f"\n=== 每分钟消息数对比 ===")
    min_a = defaultdict(int)
    min_b = defaultdict(int)
    for r in rows_a:
        min_a[r['time'][:5]] += 1
    for r in rows_b:
        min_b[r['time'][:5]] += 1

    all_mins = sorted(set(list(min_a.keys()) + list(min_b.keys())))
    print(f"{'分钟':>6}  {'A':>6}  {'B':>6}  {'差异':>6}")
    for m in all_mins:
        a = min_a.get(m, 0)
        b = min_b.get(m, 0)
        flag = ' ***' if abs(a-b) > max(a,b)*0.2 else ''
        print(f"{m:>6}  {a:>6}  {b:>6}  {b-a:>+6}{flag}")

    # msg_id 对比（如果都有 msg_id 字段）
    has_msg_a = rows_a and 'msg_id' in rows_a[0]
    has_msg_b = rows_b and 'msg_id' in rows_b[0]
    if has_msg_a and has_msg_b:
        print(f"\n=== MSG_ID 对比 ===")
        ids_a = set(r['msg_id'] for r in rows_a if r.get('msg_id'))
        ids_b = set(r['msg_id'] for r in rows_b if r.get('msg_id'))
        shared = ids_a & ids_b
        only_a = ids_a - ids_b
        only_b = ids_b - ids_a
        print(f"A 唯一 msg_id: {len(ids_a):,}")
        print(f"B 唯一 msg_id: {len(ids_b):,}")
        print(f"共享: {len(shared):,} ({len(shared)/max(len(ids_a),1)*100:.1f}%)")
        print(f"仅A: {len(only_a):,}")
        print(f"仅B: {len(only_b):,}")

    print("\n" + "=" * 60)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("用法: python compare_tools.py <DouyinBarrage_gift.csv> <DouyinLiveWebFetcher_gift.csv>")
    else:
        main(sys.argv[1], sys.argv[2])
