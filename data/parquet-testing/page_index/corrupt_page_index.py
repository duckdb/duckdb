#!/usr/bin/env python3
"""Corrupts the page index of a Parquet file in place, for the robustness tests in test/sql/copy/parquet/page_index.

    corrupt_page_index.py offset_index <in> <out> <offset_index_offset> <page> <new_first_row_index>
        rewrites page_locations[page].first_row_index (the new value must encode to a varint of the same length)
    corrupt_page_index.py column_index_minlen <in> <out> <column_index_offset> <page>
        shortens min_values[page] by one byte (the struct shrinks by one byte, a padding byte keeps the layout)

The offsets come from parquet_metadata(): column_index_offset / offset_index_offset of the column chunk. The files
corrupt_*.parquet in this directory were made from corrupt_base.parquet (3000 rows, id BIGINT in pages of 512 rows):
    offset_index corrupt_base.parquet corrupt_first_row_index.parquet <offset_index_offset of id> 1 300
    offset_index corrupt_base.parquet corrupt_non_monotonic.parquet <offset_index_offset of id> 2 100
    column_index_minlen corrupt_base.parquet corrupt_min_length.parquet <column_index_offset of id> 0
"""
import sys


def read_varint(data, pos):
    result = 0
    shift = 0
    while True:
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7f) << shift
        if not (byte & 0x80):
            return result, pos
        shift += 7


def write_varint(value):
    out = bytearray()
    while True:
        if value < 0x80:
            out.append(value)
            return bytes(out)
        out.append((value & 0x7f) | 0x80)
        value >>= 7


def zigzag_decode(value):
    return (value >> 1) ^ -(value & 1)


def zigzag_encode(value):
    return (value << 1) ^ (value >> 63)


def skip_field(data, pos, field_type):
    if field_type in (1, 2):
        return pos
    if field_type == 3:
        return pos + 1
    if field_type in (4, 5, 6):
        return read_varint(data, pos)[1]
    if field_type == 7:
        return pos + 8
    if field_type == 8:
        length, pos = read_varint(data, pos)
        return pos + length
    if field_type == 9:
        header = data[pos]
        pos += 1
        count = header >> 4
        element_type = header & 0xf
        if count == 15:
            count, pos = read_varint(data, pos)
        for _ in range(count):
            pos = skip_field(data, pos, element_type)
        return pos
    if field_type == 12:
        return read_struct(data, pos)[1]
    raise ValueError("unsupported thrift type %d" % field_type)


def read_struct(data, pos):
    """{field id: (type, position of the value)} and the position after the struct."""
    fields = {}
    last_id = 0
    while True:
        header = data[pos]
        pos += 1
        if header == 0:
            return fields, pos
        delta = header >> 4
        field_type = header & 0xf
        if delta == 0:
            field_id, pos = read_varint(data, pos)
            field_id = zigzag_decode(field_id)
        else:
            field_id = last_id + delta
        last_id = field_id
        fields[field_id] = (field_type, pos)
        pos = skip_field(data, pos, field_type)


def list_elements(data, pos):
    header = data[pos]
    pos += 1
    count = header >> 4
    element_type = header & 0xf
    if count == 15:
        count, pos = read_varint(data, pos)
    positions = []
    for _ in range(count):
        positions.append(pos)
        pos = skip_field(data, pos, element_type)
    return element_type, positions, pos


def main():
    mode, source, target = sys.argv[1], sys.argv[2], sys.argv[3]
    data = bytearray(open(source, "rb").read())
    if mode == "offset_index":
        offset, page, new_value = int(sys.argv[4]), int(sys.argv[5]), int(sys.argv[6])
        fields, _ = read_struct(data, offset)
        _, pages, _ = list_elements(data, fields[1][1])
        location_fields, _ = read_struct(data, pages[page])
        _, pos = location_fields[3]
        old, end = read_varint(data, pos)
        new = write_varint(zigzag_encode(new_value))
        assert len(new) == end - pos, "the new value must encode to %d bytes" % (end - pos)
        print("page %d first_row_index %d -> %d" % (page, zigzag_decode(old), new_value))
        data[pos:end] = new
    elif mode == "column_index_minlen":
        offset, page = int(sys.argv[4]), int(sys.argv[5])
        fields, end = read_struct(data, offset)
        _, mins, _ = list_elements(data, fields[2][1])
        pos = mins[page]
        length, value_pos = read_varint(data, pos)
        assert 1 < length < 0x80
        print("min_values[%d] length %d -> %d" % (page, length, length - 1))
        data[pos] = length - 1
        del data[value_pos + length - 1]
        data.insert(end - 1, 0)
    else:
        raise SystemExit("unknown mode " + mode)
    open(target, "wb").write(data)


if __name__ == "__main__":
    main()
