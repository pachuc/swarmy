import os
import struct
import tempfile
import unittest
import zlib
from png_check import check, SIGNATURE
from desktop_check_app import GLYPHS, PAD, CELL


def make_png(w, h, rows, filter_type=0, channels=3):
    raw = bytearray()
    previous = bytes(w * channels)
    for pixels in rows:
        encoded = bytearray(pixels)
        for i, value in enumerate(pixels):
            left = pixels[i - channels] if i >= channels else 0
            up = previous[i]
            ul = previous[i - channels] if i >= channels else 0
            p = left + up - ul
            distances = (abs(p-left), abs(p-up), abs(p-ul))
            predictor = (0, left, up, (left+up)//2,
                         (left, up, ul)[distances.index(min(distances))])[filter_type]
            encoded[i] = (value - predictor) & 255
        raw.append(filter_type)
        raw.extend(encoded)
        previous = pixels
    def chunk(kind, payload):
        return struct.pack('>I', len(payload)) + kind + payload + struct.pack('>I', zlib.crc32(kind + payload))
    return (SIGNATURE + chunk(b'IHDR', struct.pack('>IIBBBBB', w, h, 8, 6 if channels == 4 else 2, 0, 0, 0))
            + chunk(b'IDAT', zlib.compress(raw)) + chunk(b'IEND', b''))


def glyph_pixels(code, x0, y0):
    return {(x0 + (PAD + i * 6 + col) * CELL + dx,
             y0 + (PAD + row) * CELL + dy)
            for i, char in enumerate(code)
            for row, line in enumerate(GLYPHS[char])
            for col, bit in enumerate(line) if bit == '1'
            for dx in range(CELL) for dy in range(CELL)}


def glyph_image(pixels):
    return make_png(520, 180, [b''.join(
        b'\x11\x11\x11' if (x, y) in pixels else b'\xff\xff\xff'
        for x in range(520)) for y in range(180)])


class PngTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, 'image.png')

    def write(self, data):
        with open(self.path, 'wb') as out:
            out.write(data)
        return self.path

    def test_blank(self):
        with self.assertRaisesRegex(ValueError, 'blank'):
            check(self.write(make_png(64, 40, [bytes([255,255,255])*64]*40)))

    def test_filters_and_size(self):
        rows = [bytes([0,0,0])*8 + bytes([255,255,255])*8 for _ in range(12)]
        expected = check(self.write(make_png(16, 12, rows)))
        self.assertEqual(expected['colors'], 2)
        for filter_type in range(1, 5):
            with self.subTest(filter_type=filter_type):
                self.assertEqual(check(self.write(make_png(16, 12, rows, filter_type))), expected)
        with self.assertRaisesRegex(ValueError, 'size 16x12'):
            check(self.path, width=17, height=12)

    def test_expected_code_pixels(self):
        code = 'ACEMX3'
        x0, y0 = 20, 18
        lit = glyph_pixels(code, x0, y0)
        self.assertEqual(check(self.write(glyph_image(lit)), expect_code=code)['width'], 520)
        # Change a cell inside the glyph so its outer bounds stay unchanged.
        flipped = {(x0 + (PAD + 3) * CELL + dx,
                    y0 + (PAD + 3) * CELL + dy)
                   for dx in range(CELL) for dy in range(CELL)}
        with self.assertRaisesRegex(ValueError, 'code not visible'):
            check(self.write(glyph_image(lit - flipped)), expect_code=code)

    def test_stray_dark_pixel_outside_canvas(self):
        code = 'ACEMX3'
        lit = glyph_pixels(code, 20, 18)
        # The pixel is in browser chrome, beyond the candidate canvas bounds.
        self.assertEqual(check(self.write(glyph_image(lit | {(0, 0)})),
                               expect_code=code)['width'], 520)

    def test_rgba(self):
        rows = [bytes([0,0,0,255])*4 + bytes([255,255,255,255])*4 for _ in range(8)]
        self.assertEqual(check(self.write(make_png(8, 8, rows, channels=4)))['width'], 8)

    def test_not_png(self):
        with self.assertRaisesRegex(ValueError, 'not a PNG'):
            check(self.write(b'hello'))
