import zstandard


cctx = zstandard.ZstdCompressor()
with open('zstd_compressed.json.zst', 'wb') as fh:
    fh.write(cctx.compress(b'{"key": "value"}'))
