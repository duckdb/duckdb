import os
version = '3.6.4'

# os.system(f'wget https://github.com/Mbed-TLS/mbedtls/archive/refs/tags/mbedtls-{version}.tar.gz')
# os.system(f'tar xvf mbedtls-{version}.tar.gz')

directories = ['include', 'library']
source_dir = f'mbedtls-mbedtls-{version}'
target_dir = '.'
extensions = ['.h', '.hpp', '.c', '.cpp']

class FileToCopy:
    def __init__(self, source, target):
        self.source_file = source
        self.target_file = target

def get_copy_list(source_dir, target_dir):
    result = []
    for file in os.listdir(source_dir):
        is_source_file = False
        for ext in extensions:
            if file.endswith(ext):
                is_source_file = True
        if not is_source_file:
            continue
        target_file_name = file
        if target_file_name.endswith('.c'):
            target_file_name = target_file_name[:-2] + '.cpp'
        source_file = os.path.join(source_dir, file)
        target_file = os.path.join(target_dir, target_file_name)
        if os.path.isdir(source_file):
            result += get_copy_list(source_file, target_file)
        if not os.path.isfile(target_file):
            continue
        # check if this is a dummy file
        with open(target_file, 'r') as f:
            text = f.read()
        if '// dummy file' in text:
            continue
        print(target_file)
        result.append(FileToCopy(source_file, target_file))
    return result


copy_list = []
for directory in directories:
    copy_list += get_copy_list(os.path.join(source_dir, directory), os.path.join(target_dir, directory))

for file in copy_list:
    os.system(f'cp {file.source_file} {file.target_file}')