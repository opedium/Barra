"""Decode BattleEndPunish body structure."""
import sys, os
sys.stdout.reconfigure(encoding='utf-8')

def read_varint(data, pos):
    value = 0; shift = 0
    while pos < len(data):
        b = data[pos]; pos += 1
        value |= (b & 0x7f) << shift
        if not (b & 0x80): return value, pos
        shift += 7
    return None, pos

def read_ld(data, pos):
    length, pos = read_varint(data, pos)
    if length is None or pos + length > len(data): return None, pos
    return data[pos:pos + length], pos + length

def dump(data, indent=0):
    prefix = "  " * indent
    pos = 0
    while pos < len(data):
        tag, pos = read_varint(data, pos)
        if tag is None: break
        fn = tag >> 3; wt = tag & 0x7
        if wt == 0:
            val, pos = read_varint(data, pos)
            print(f"{prefix}f{fn}(v) = {val}")
        elif wt == 2:
            raw, pos = read_ld(data, pos)
            if raw is None: continue
            s = None
            try:
                s = raw.decode('utf-8')
                if s.isprintable() and len(s) <= 120:
                    print(f"{prefix}f{fn}(s) = \"{s}\"")
                    continue
            except: pass
            print(f"{prefix}f{fn}(L) = {len(raw)} bytes")
            if len(raw) <= 1000: dump(raw, indent + 1)
        elif wt == 5:
            pos += 4
            print(f"{prefix}f{fn}(f32)")

dump_dir = "G:\\DouyinBarrage-main\\data\\922832693059\\20260527_0057_7644197185566886707\\pk_dumps"
for f in sorted(os.listdir(dump_dir)):
    if 'BattleEndPunish' in f or 'PowerContainer' in f:
        path = os.path.join(dump_dir, f)
        with open(path, 'rb') as fh: data = fh.read()
        print(f"\n=== {f} ({len(data)} bytes) ===")
        dump(data)
