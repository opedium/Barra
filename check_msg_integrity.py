#!/usr/bin/python
# coding:utf-8
"""
消息完整性检测 + 重复分析工具
用法: python check_msg_integrity.py <gift.csv路径>

检测内容:
1. msg_id 重复率（WebSocket 重传）
2. 重复消息对钻石数的影响
3. 消息时间间隔（检测是否有消息丢失时段）
4. 非单调 combo 中的 msg_id 分布
5. 去重方式对比汇总
"""

import sys
import csv
from collections import defaultdict, Counter

def analyze(file_path):
    print("=" * 60)
    print(f"  消息完整性分析: {file_path}")
    print("=" * 60)

    rows = []
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        for r in csv.DictReader(f):
            rows.append(r)
    print(f"\n总行数: {len(rows)}")

    # ── 1. msg_id 分析 ──
    has_field = rows and 'msg_id' in rows[0]
    msg_ids_all = []
    msg_id_rows = []
    empty_count = 0
    for r in rows:
        mid = r.get('msg_id', '').strip()
        if mid and mid != '0':
            msg_ids_all.append(mid)
            msg_id_rows.append(r)
        else:
            empty_count += 1

    print(f"\n{'='*40}")
    print(f"  MSG_ID 分析")
    print(f"{'='*40}")
    print(f"有 msg_id: {len(msg_ids_all)} 行")
    print(f"无 msg_id (空/0): {empty_count} 行")

    if not msg_ids_all:
        print("\n⚠ 所有行的 msg_id 都为空！")
        print("  可能原因: 抖音服务器未设置 common.msg_id 字段")
        print("  回退方案: 使用 (time+user_id+gift_name+group_id) 组合做去重检测")
    else:
        unique_ids = set(msg_ids_all)
        dup_count = len(msg_ids_all) - len(unique_ids)
        print(f"唯一 msg_id: {len(unique_ids)}")
        print(f"重复 msg_id: {dup_count} ({dup_count/len(msg_ids_all)*100:.2f}%)" if msg_ids_all else "")

        if dup_count > 0:
            id_counts = Counter(msg_ids_all)
            dup_ids = {mid: cnt for mid, cnt in id_counts.items() if cnt > 1}
            print(f"\n重复的 msg_id 种类: {len(dup_ids)}")

            # 重复消息的钻石影响
            dup_diamond_total = 0
            for mid, cnt in dup_ids.items():
                matching = [r for r in msg_id_rows if r.get('msg_id') == mid]
                if matching:
                    max_d = max(int(r['diamond_total']) for r in matching)
                    sum_d = sum(int(r['diamond_total']) for r in matching)
                    dup_diamond_total += (sum_d - max_d)

            print(f"重复消息多算的钻石: {dup_diamond_total:,}")

            # 展示前几个重复
            print(f"\n重复示例 (前5个):")
            for mid, cnt in list(dup_ids.items())[:5]:
                matching = [r for r in msg_id_rows if r.get('msg_id') == mid]
                print(f"  msg_id={mid}: 出现 {cnt} 次")
                for m in matching[:3]:
                    print(f"    {m['time']} {m['user_name']} {m['gift_name']} cnt={m['gift_count']} diamond={m['diamond_total']} end={m.get('raw_repeat_end','')}")

    # ── 2. 时间分布 ──
    print(f"\n{'='*40}")
    print(f"  时间分布")
    print(f"{'='*40}")
    time_buckets = defaultdict(int)
    for r in rows:
        minute = r['time'][:5]
        time_buckets[minute] += 1

    peak_min = max(time_buckets, key=time_buckets.get)
    print(f"时间范围: {min(time_buckets)} - {max(time_buckets)}")
    print(f"高峰分钟: {peak_min} ({time_buckets[peak_min]} 条)")

    for minute in sorted(time_buckets.keys()):
        cnt = time_buckets[minute]
        bar = '#' * (cnt // 20 + 1)
        print(f"  {minute}: {cnt:>5} {bar}")

    # ── 3. 消息间隔 ──
    print(f"\n{'='*40}")
    print(f"  消息间隔检测 (>=5秒)")
    print(f"{'='*40}")
    gaps = []
    max_gap = 0
    max_gap_info = ('', '', 0)
    for i in range(1, len(rows)):
        prev_t = rows[i-1]['time']
        curr_t = rows[i]['time']
        try:
            prev_sec = int(prev_t[0:2])*3600 + int(prev_t[3:5])*60 + int(prev_t[6:8])
            curr_sec = int(curr_t[0:2])*3600 + int(curr_t[3:5])*60 + int(curr_t[6:8])
            gap = curr_sec - prev_sec if curr_sec >= prev_sec else 0
            if gap > max_gap:
                max_gap = gap
                max_gap_info = (prev_t, curr_t, gap)
            if gap >= 5:
                gaps.append((prev_t, curr_t, gap))
        except ValueError:
            pass

    print(f">=5秒的间隔: {len(gaps)} 个")
    print(f"最大间隔: {max_gap}秒 ({max_gap_info[0]} → {max_gap_info[1]})")
    if gaps:
        print(f"\n前10个间隔:")
        for prev, curr, gap in gaps[:10]:
            print(f"  {prev} → {curr}: {gap}秒")

    # ── 4. 非单调 combo 分析 ──
    print(f"\n{'='*40}")
    print(f"  非单调 Combo 分析")
    print(f"{'='*40}")

    combo_map = defaultdict(list)
    for r in rows:
        gid = r.get('group_id','')
        uid = r.get('user_id','')
        if gid and gid != '0':
            combo_map[f'{uid}_{gid}_{r["gift_name"]}'].append(r)

    non_mono = []
    for ck, entries in combo_map.items():
        if len(entries) < 3:
            continue
        sorted_e = sorted(entries, key=lambda e: e['time'])
        counts = [int(e['gift_count']) for e in sorted_e]
        if not all(counts[i] <= counts[i+1] for i in range(len(counts)-1)):
            non_mono.append((ck, sorted_e, counts))

    print(f"非单调 combo 数: {len(non_mono)}")
    if non_mono:
        # 这些非单调 combo 的 msg_id
        nm_msg_ids = []
        for _, entries, _ in non_mono:
            for e in entries:
                mid = e.get('msg_id', '').strip()
                if mid and mid != '0':
                    nm_msg_ids.append(mid)
        if nm_msg_ids:
            nm_dup = len(nm_msg_ids) - len(set(nm_msg_ids))
            print(f"非单调 combo 中的 msg_id 重复: {nm_dup}")

        print(f"\n前3个非单调 combo:")
        for ck, entries, counts in non_mono[:3]:
            print(f"  {ck}: counts={counts}")
            for e in entries[:8]:
                print(f"    {e['time']} cnt={e['gift_count']} diamond={e['diamond_total']} end={e.get('raw_repeat_end','')} msg_id={e.get('msg_id','?')}")

    # ── 5. 汇总 ──
    print(f"\n{'='*40}")
    print(f"  钻石汇总对比")
    print(f"{'='*40}")
    raw_sum = sum(int(r['diamond_total']) for r in rows)
    dedup_sum = sum(
        max(int(e['diamond_total']) for e in entries)
        for entries in combo_map.values()
    )
    print(f"原始行求和:      {raw_sum:>12,}")
    print(f"uid+gid+gift max: {dedup_sum:>12,}")
    print(f"差值 (去重损失):   {raw_sum - dedup_sum:>12,} ({(1-dedup_sum/raw_sum)*100:.1f}%)")

    print("\n" + "=" * 60)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("用法: python check_msg_integrity.py <gift.csv>")
    else:
        analyze(sys.argv[1])
