
import os

common_path = os.path.join('src', 'include', 'duckdb', 'common', 'common.hpp')
with open(common_path, 'r') as f:
	text = f.read()


text = text.replace('#pragma once', '''#pragma once

#include "duckdb/common/windows.hpp"
''')

with open(common_path, 'w+') as f:
	f.write(text)
