"""Extract all fields from BattleEndPunish and BattlePowerContainer dumps."""
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

def dump_fields(data, indent=0):
    """Dump all fields with their types."""
    prefix = "  " * indent
    pos = 0
    while pos < len(data):
        tag, pos = read_varint(data, pos)
        if tag is None:
            break
        fn = tag >> 3
        wt = tag & 0x7
        if wt == 0:
            val, pos = read_varint(data, pos)
            print(f"{prefix}f{fn} (varint): {val}")
        elif wt == 2:
            raw, pos = read_ld(data, pos)
            if raw is None:
                print(f"{prefix}f{fn} (ld): <truncated>")
                continue
            try:
                s = raw.decode('utf-8')
                if len(s) <= 100 and s.isprintable():
                    print(f"{prefix}f{fn} (string): \"{s}\"")
                    continue
            except Exception:
                pass
            print(f"{prefix}f{fn} (ld): {len(raw)} bytes")
            if len(raw) < 500 and fn not in (1,):
                dump_fields(raw, indent + 1)
            elif len(raw) > 0 and fn in (1, 15):
                dump_fields(raw, indent + 1)
        elif wt == 5:
            import struct
            if pos + 4 <= len(data):
                val = struct.unpack('<f', data[pos:pos+4])[0]
                print(f"{prefix}f{fn} (float): {val}")
                pos += 4
        else:
            print(f"{prefix}f{fn} (wt={wt}): <skipped>")

def main():
    dump_dir = sys.argv[1]
    for f in sorted(os.listdir(dump_dir)):
        if 'UpdateScore' not in f:
            path = os.path.join(dump_dir, f)
            with open(path, 'rb') as fh:
                data = fh.read()
            print(f"\n{'='*80}")
            print(f"FILE: {f}  ({len(data)} bytes)")
            print(f"{'='*80}")
            dump_fields(data)
            print()

if __name__ == '__main__':
    main()
