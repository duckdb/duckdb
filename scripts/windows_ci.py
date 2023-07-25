import os

common_path = os.path.join('src', 'include', 'duckdb', 'common', 'common.hpp')
with open(common_path, 'r') as f:
    text = f.read()


text = text.replace(
    '#pragma once',
    '''#pragma once

#ifdef _WIN32
#ifdef DUCKDB_MAIN_LIBRARY
#include "duckdb/common/windows.hpp"
#endif
#endif
''',
)

with open(common_path, 'w+') as f:
    f.write(text)
