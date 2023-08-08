import os

# list all include directories
include_directories = [os.path.sep.join(x.split('/')) for x in ['extension/visualizer/include']]
# source files
source_files = [os.path.sep.join(x.split('/')) for x in ['extension/visualizer/visualizer_extension.cpp']]
