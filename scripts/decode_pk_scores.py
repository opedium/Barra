"""Extract PK scores from UpdateScore raw protobuf dumps."""
import sys, os
sys.stdout.reconfigure(encoding='utf-8')

def read_varint(data, pos):
    value = 0
    shift = 0
    while pos < len(data):
        b = data[pos]; pos += 1
        value |= (b & 0x7f) << shift
        if not (b & 0x80):
            return value, pos
        shift += 7
    return None, pos

def read_ld(data, pos):
    length, pos = read_varint(data, pos)
    if length is None or pos + length > len(data):
        return None, pos
    return data[pos:pos + length], pos + length

def extract_fields(data):
    """Extract key varint fields and linker_id string at top level."""
    result = {}
    pos = 0
    while pos < len(data):
        tag, pos = read_varint(data, pos)
        if tag is None:
            break
        fn = tag >> 3
        wt = tag & 0x7
        if wt == 0:
            val, pos = read_varint(data, pos)
            result[f"f{fn}"] = val
        elif wt == 2:
            raw, pos = read_ld(data, pos)
            if raw is None:
                continue
            if fn == 10:
                try:
                    result['linker_id'] = raw.decode('utf-8')
                except Exception:
                    pass
        elif wt == 5:
            pos += 4
        # skip other wire types
    return result

def main():
    dump_dir = sys.argv[1]
    files = sorted(f for f in os.listdir(dump_dir) if 'UpdateScore' in f)

    print(f"{'time':<8} {'size':>6}  {'score_self':>15}  {'score_other':>15}  {'room_id':<20}  {'ts_ms':>15}  linker_id")
    print("-" * 150)

    for fname in files:
        path = os.path.join(dump_dir, fname)
        with open(path, 'rb') as fh:
            data = fh.read()

        fields = extract_fields(data)
        time_str = fname.split('_')[0]
        ts_raw = f"{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"

        score_self = fields.get('f6', '?')
        score_other = fields.get('f7', '?')
        room_id = fields.get('f4', '?')
        ts = fields.get('f11', '?')
        linker = fields.get('linker_id', '')

        print(f"{ts_raw:<8} {len(data):>6}  {str(score_self):>15}  {str(score_other):>15}  {str(room_id):<20}  {str(ts):>15}  {linker}")

if __name__ == '__main__':
    main()
