"""Extract numeric values from raw PK protobuf dumps."""
import sys, os, struct

def read_varint(data, pos):
    """Read a protobuf varint, return (value, new_pos)."""
    value = 0
    shift = 0
    while pos < len(data):
        b = data[pos]
        pos += 1
        value |= (b & 0x7f) << shift
        if not (b & 0x80):
            return value, pos
        shift += 7
    return None, pos

def read_length_delimited(data, pos):
    """Read a length-delimited field, return (bytes, new_pos)."""
    length, pos = read_varint(data, pos)
    if length is None or pos + length > len(data):
        return None, pos
    return data[pos:pos + length], pos + length

def decode_proto(data, indent=0):
    """Recursively decode a protobuf message, returning list of (field_num, wire_type, value_str, bytes_val)."""
    results = []
    pos = 0
    prefix = "  " * indent
    while pos < len(data):
        tag, pos = read_varint(data, pos)
        if tag is None:
            break
        field_num = tag >> 3
        wire_type = tag & 0x7

        if wire_type == 0:  # varint
            val, pos = read_varint(data, pos)
            results.append((field_num, wire_type, str(val), None))
        elif wire_type == 2:  # length-delimited
            raw, pos = read_length_delimited(data, pos)
            if raw is None:
                results.append((field_num, wire_type, "<truncated>", None))
                continue
            # Try to decode as string
            try:
                s = raw.decode('utf-8')
                if len(s) > 80:
                    s = s[:80] + "..."
                if s.isprintable() and len(s) > 1:
                    results.append((field_num, wire_type, f'"{s}"', raw))
                    continue
            except:
                pass
            # Try nested message
            if len(raw) > 0:
                nested = decode_proto(raw, indent + 1)
                results.append((field_num, wire_type, f"<message len={len(raw)}>", raw))
                results.extend(nested)
            else:
                results.append((field_num, wire_type, f'""', raw))
        elif wire_type == 5:  # 32-bit
            if pos + 4 <= len(data):
                val = struct.unpack('<f', data[pos:pos+4])[0]
                results.append((field_num, wire_type, str(val), data[pos:pos+4]))
                pos += 4
        else:
            results.append((field_num, wire_type, f"<wire_type={wire_type}>", None))
    return results

def find_all_numbers(data, label=""):
    """Extract all varint and float values with their field numbers."""
    results = decode_proto(data)
    numbers = []
    for field_num, wire_type, val_str, raw in results:
        if wire_type in (0, 5):
            numbers.append((field_num, val_str))
    return numbers

def main():
    dump_dir = sys.argv[1] if len(sys.argv) > 1 else None
    if not dump_dir:
        print("Usage: python decode_pk_dumps.py <pk_dumps_dir>")
        return

    files = sorted(os.listdir(dump_dir))
    for f in files:
        if 'UpdateScore' not in f:
            continue
        path = os.path.join(dump_dir, f)
        with open(path, 'rb') as fh:
            data = fh.read()

        print(f"\n{'='*80}")
        print(f"FILE: {f}  ({len(data)} bytes)")
        print(f"{'='*80}")

        results = decode_proto(data)
        # Show structure 2 levels deep (indent prefix counts)
        for field_num, wire_type, val_str, raw in results:
            depth = 0  # We'll track depth differently
            print(f"  field {field_num} (wt={wire_type}): {val_str}")

if __name__ == '__main__':
    main()
