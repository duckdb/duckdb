def open_utf8(fpath, flags):
    import sys

    if sys.version_info[0] < 3:
        return open(fpath, flags)
    else:
        return open(fpath, flags, encoding="utf8")


def normalize_path(path):
    import os

    def normalize(p):
        return os.path.sep.join(p.split('/'))

    if isinstance(path, list):
        normed = map(lambda p: normalize(p), path)
        return list(normed)

    if (isinstance, str):
        return normalize(path)

    raise Exception("Can only be called with a str or list argument")
