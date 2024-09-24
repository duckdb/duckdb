import glob, os

prefix = os.path.join('extension', 'core_functions')

include_directories = [os.path.join(prefix, x) for x in ['include']]
source_files = [os.path.join(prefix, x) for x in glob.glob("**.cpp", root_dir=prefix, recursive=True)]
