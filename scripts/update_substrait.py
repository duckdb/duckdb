# Requires buf
import os
import shutil

def generate_buf_gen(path):
	content = '''version: v1
plugins:
  - name: cpp
    out: gen/proto/cpp'''
	f = open(os.path.join(path,'buf.gen.yaml'), "w")
	f.write(content)
	f.close()

# Change to substrait folder
sub_folder  = os.path.join(os.path.dirname(os.path.realpath(__file__)),'..','third_party','substrait')
os.chdir(sub_folder)

# Clone Proto Files
os.system("git clone https://github.com/substrait-io/substrait")
git_folder =  os.path.join(sub_folder,'substrait')
git_proto_folder = os.path.join(git_folder, 'proto')

shutil.rmtree('proto')
shutil.move(git_proto_folder, sub_folder)
generate_buf_gen(os.path.join(sub_folder,'proto'))

shutil.rmtree(git_folder)

# Generate CPP Files from Proto Files
os.chdir(os.path.join(sub_folder,'proto'))

os.system('buf generate')