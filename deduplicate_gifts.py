import csv
import os

def deduplicate_gift_data(input_file, output_file):
    gift_registry = {}
    total_raw_rows = 0
    has_repeat_end = False

    if not os.path.exists(input_file):
        print(f"Not found: {input_file}")
        return

    print(f"Processing: {input_file} ...")

    with open(input_file, mode='r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        if fieldnames and 'raw_repeat_end' in fieldnames:
            has_repeat_end = True

        for row in reader:
            total_raw_rows += 1
            gid = row.get('group_id')
            uid = row.get('user_id')
            gift = row.get('gift_name', '')

            if not gid or gid == '' or gid == '0':
                temp_id = f"single_{row['time']}_{row['user_name']}_{gift}"
                gift_registry[temp_id] = row
                continue

            # 三键组合：user_id + group_id + gift_name
            combo_id = f"{uid}_{gid}_{gift}"

            # 如果数据包含 repeat_end 字段，只取终态行
            if has_repeat_end:
                end_val = row.get('raw_repeat_end', '')
                if end_val == '':
                    pass
                elif int(end_val) == 0:
                    continue

            if combo_id not in gift_registry:
                gift_registry[combo_id] = row
            else:
                cur = int(row.get('diamond_total', 0))
                sto = int(gift_registry[combo_id].get('diamond_total', 0))
                if cur > sto:
                    gift_registry[combo_id] = row

    with open(output_file, mode='w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(gift_registry.values())

    final_diamonds = sum(int(r.get('diamond_total', 0)) for r in gift_registry.values())

    print("-" * 40)
    print(f"Done.")
    print(f"  Raw rows: {total_raw_rows}")
    print(f"  Deduplicated rows: {len(gift_registry)}")
    print(f"  Total diamonds: {final_diamonds}")
    if has_repeat_end:
        print(f"  (repeat_end filter: ON)")
    print(f"  Output: {output_file}")
    print("-" * 40)

if __name__ == "__main__":
    target = input("Path to gift.csv: ").strip().strip('"')
    output = target.replace('.csv', '_cleaned.csv')
    deduplicate_gift_data(target, output)
    input("Press Enter to exit...")
