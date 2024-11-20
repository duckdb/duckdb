import os

prefix = os.path.join('extension', 'core_functions')


def list_files_recursive(rootdir, suffix):
    file_list = []
    for root, _, files in os.walk(rootdir):
        file_list += [os.path.join(root, f) for f in files if f.endswith(suffix)]
    return file_list


include_directories = [os.path.join(prefix, x) for x in ['include']]
source_files = list_files_recursive(prefix, '.cpp')
