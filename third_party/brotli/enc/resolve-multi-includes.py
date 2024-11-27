# brotli uses a weird c templating mechanism using _inc.h files
# this does not play well with things like amalagamation
# this script inlines the variuos headers

import os
import re

for filename in os.listdir('.'):
    if not (filename.endswith('.cpp') or filename.endswith('.h')): 
        continue

    file_lines = open(filename, 'r').readlines()
    if '_inc.h' not in '\n'.join(file_lines):
        continue

    out = open (filename, 'w')

    for line in file_lines:
        if '#include' in line and '_inc.h' in line:
            match = re.search(r'#include\s+"(.+)".*', line).group(1)
            include = open(match, 'r').readlines();
            out.write(''.join(include))
            continue
        out.write(line)
