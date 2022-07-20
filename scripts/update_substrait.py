# Requires protoc 3.19.04
# https://github.com/protocolbuffers/protobuf/releases/tag/v3.19.4

import os
import shutil
from os import walk

GITHUB_TAG = "b8fb06a52397463bfe9cffc2c89fe71eba56b2ca" # V0.8
# Change to substrait folder
sub_folder  = os.path.join(os.path.dirname(os.path.realpath(__file__)),'..','third_party','substrait')
os.chdir(sub_folder)


# Delete Current CPP files
shutil.rmtree(os.path.join(sub_folder,'substrait'))

# Clone Proto Files
os.system("git clone https://github.com/substrait-io/substrait git-sub")
git_folder =  os.path.join(sub_folder,'git-sub')

# Generate Proto Files on a specific git tag
os.chdir(git_folder)
os.system("git checkout " + GITHUB_TAG)
os.chdir(sub_folder)

proto_folder =  os.path.join(git_folder,'proto')
substrait_proto_folder = os.path.join(proto_folder,'substrait')
substrait_extensions_proto_folder = os.path.join(substrait_proto_folder,'extensions')

os.mkdir("substrait")
os.mkdir("substrait/extensions")

# Generate all files
proto_sub_list = next(walk(substrait_proto_folder), (None, None, []))[2]

proto_sub_extensions = next(walk(substrait_extensions_proto_folder), (None, None, []))[2]

print("Protoc version" + os.popen('protoc --version').read())

for proto in proto_sub_list:
    os.system("protoc -I="+ proto_folder+ " --cpp_out="+sub_folder +" "+ os.path.join(substrait_proto_folder,proto))

for proto in proto_sub_extensions:
    os.system("protoc -I="+ proto_folder+ " --cpp_out="+sub_folder +" "+ os.path.join(substrait_extensions_proto_folder,proto))

# Delete Git Folder
shutil.rmtree(git_folder)
