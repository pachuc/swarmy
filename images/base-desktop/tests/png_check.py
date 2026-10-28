"""Decode PNG pixels without external dependencies and reject blank screenshots."""
import argparse
import collections
import json
import struct
import sys
import zlib

from desktop_check_app import GLYPHS, PAD, CELL, W, H

SIGNATURE = b'\x89PNG\r\n\x1a\n'


def check(path, *, width=None, height=None, min_width=None, min_height=None,
          max_width=None, max_height=None, max_dominant=0.995,
          expect_code=None, kind=None):
    if kind == 'screen':
        width, height = 1280, 800
    elif kind == 'browser':
        min_width, min_height, max_width, max_height = 320, 200, 1280, 800
    elif kind is not None:
        raise ValueError(f'unknown screenshot kind: {kind}')
    with open(path, 'rb') as source:
        data = source.read()
    if not data.startswith(SIGNATURE):
        raise ValueError('not a PNG')
    offset, compressed, header = 8, bytearray(), None
    while offset + 12 <= len(data):
        size = struct.unpack_from('>I', data, offset)[0]
        kind = data[offset + 4:offset + 8]
        end = offset + 12 + size
        if end > len(data):
            raise ValueError('truncated PNG chunk')
        payload = data[offset + 8:offset + 8 + size]
        if zlib.crc32(kind + payload) != struct.unpack_from('>I', data, end - 4)[0]:
            raise ValueError('PNG chunk CRC mismatch')
        if kind == b'IHDR':
            if size != 13:
                raise ValueError('unsupported PNG: IHDR length')
            w, h, depth, color, compression, filtering, interlace = struct.unpack('>IIBBBBB', payload)
            if depth != 8 or color not in (0, 2, 6) or compression or filtering or interlace:
                raise ValueError(f'unsupported PNG: depth={depth} color={color} interlace={interlace}')
            header = w, h, {0: 1, 2: 3, 6: 4}[color]
        elif kind == b'IDAT':
            compressed.extend(payload)
        elif kind == b'IEND':
            break
        offset = end
    if header is None or not compressed:
        raise ValueError('not a PNG: missing IHDR or IDAT')
    w, h, channels = header
    if w == 0 or h == 0 or any((limit is not None and wrong) for limit, wrong in (
            (width, w != width), (height, h != height),
            (min_width, w < (min_width or 0)), (min_height, h < (min_height or 0)),
            (max_width, w > (max_width or w)), (max_height, h > (max_height or h)))):
        raise ValueError(f'size {w}x{h}, expected width={width} height={height} '
                         f'min={min_width}x{min_height} max={max_width}x{max_height}')
    raw = zlib.decompress(compressed)
    stride = w * channels
    if len(raw) != h * (stride + 1):
        raise ValueError('unsupported PNG: decompressed size')
    colors = collections.Counter()
    dark = set()
    previous = bytearray(stride)
    for y in range(h):
        start = y * (stride + 1)
        filter_type = raw[start]
        if filter_type > 4:
            raise ValueError(f'unsupported PNG: filter {filter_type}')
        row = bytearray(raw[start + 1:start + 1 + stride])
        for i in range(stride):
            left = row[i - channels] if i >= channels else 0
            up = previous[i]
            upper_left = previous[i - channels] if i >= channels else 0
            if filter_type == 1:
                predictor = left
            elif filter_type == 2:
                predictor = up
            elif filter_type == 3:
                predictor = (left + up) // 2
            elif filter_type == 4:
                p = left + up - upper_left
                distances = (abs(p - left), abs(p - up), abs(p - upper_left))
                predictor = (left, up, upper_left)[distances.index(min(distances))]
            else:
                predictor = 0
            row[i] = (row[i] + predictor) & 255
        for x in range(w):
            pixel = bytes(row[x * channels:(x + 1) * channels])
            colors[pixel] += 1
            if expect_code is not None and pixel[:3] == b'\x11\x11\x11':
                dark.add((x, y))
        previous = row
    dominant = colors.most_common(1)[0][1] / (w * h)
    if len(colors) < 2 or dominant > max_dominant:
        raise ValueError(f'blank: one color covers {dominant:.2%}')
    if expect_code is not None:
        check_code(dark, expect_code, w, h)
    return dict(width=w, height=h, colors=len(colors), dominant_fraction=dominant)


def check_code(dark, code, width, height):
    if len(code) != 6 or any(char not in GLYPHS for char in code) or not dark:
        raise ValueError('code not visible: missing glyph pixels or invalid code')
    expected = {(PAD + i * 6 + col, PAD + row)
                for i, char in enumerate(code)
                for row, line in enumerate(GLYPHS[char])
                for col, bit in enumerate(line) if bit == '1'}
    # The first top-row cell has a top-left corner. Locate candidate canvas
    # origins from that corner, then ignore dark browser chrome outside W x H.
    anchor_x, anchor_y = min(expected, key=lambda cell: (cell[1], cell[0]))
    last_x, last_y = max(expected, key=lambda cell: (cell[0], cell[1]))
    for x, y in dark:
        if (x - 1, y) in dark or (x, y - 1) in dark:
            continue
        origin_x, origin_y = x - anchor_x * CELL, y - anchor_y * CELL
        if not (0 <= origin_x <= width - W and 0 <= origin_y <= height - H):
            continue
        if (origin_x + last_x * CELL + CELL // 2,
                origin_y + last_y * CELL + CELL // 2) not in dark:
            continue
        canvas = {(px, py) for px, py in dark
                  if origin_x <= px < origin_x + W and origin_y <= py < origin_y + H}
        try:
            check_glyph_shape(canvas, expected)
        except ValueError:
            continue
        return
    raise ValueError('code not visible: glyph pixels differ within canvas')


def check_glyph_shape(dark, expected):
    x_min, x_max = min(x for x, _ in dark), max(x for x, _ in dark)
    y_min, y_max = min(y for _, y in dark), max(y for _, y in dark)
    cell_x_min = min(x for x, _ in expected)
    cell_y_min = min(y for _, y in expected)
    cell_x_max = max(x for x, _ in expected)
    cell_y_max = max(y for _, y in expected)
    if (x_max - x_min + 1, y_max - y_min + 1) != (
            (cell_x_max - cell_x_min + 1) * CELL,
            (cell_y_max - cell_y_min + 1) * CELL):
        raise ValueError('code not visible: glyph bounds differ')
    origin_x = x_min - cell_x_min * CELL
    origin_y = y_min - cell_y_min * CELL
    for row in range(7):
        for col in range(6 * 6 - 1):
            center = (origin_x + (PAD + col) * CELL + CELL // 2,
                      origin_y + (PAD + row) * CELL + CELL // 2)
            if (center in dark) != ((PAD + col, PAD + row) in expected):
                raise ValueError(f'code not visible: cell {col},{row} differs')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('file')
    for name in ('width', 'height', 'min_width', 'min_height', 'max_width', 'max_height'):
        parser.add_argument('--' + name.replace('_', '-'), type=int)
    parser.add_argument('--max-dominant', type=float, default=0.995)
    parser.add_argument('--kind', choices=('screen', 'browser'))
    parser.add_argument('--expect-code')
    args = parser.parse_args()
    try:
        print(json.dumps(check(args.file, **{k: v for k, v in vars(args).items() if k != 'file'})))
    except (ValueError, OSError, zlib.error) as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
