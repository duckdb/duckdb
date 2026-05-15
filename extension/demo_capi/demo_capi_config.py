import os

extension_kind = 'CAPI'

# list all include directories
include_directories = [os.path.sep.join(x.split('/')) for x in ['extension/demo_capi/include']]


# source files
def list_files_recursive(rootdir, suffix):
    file_list = []
    for root, _, files in os.walk(rootdir):
        file_list += [os.path.join(root, f) for f in files if f.endswith(suffix)]
    return file_list


prefix = os.path.join('extension', 'demo_capi')
source_files = list_files_recursive(prefix, '.cpp')
