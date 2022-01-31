# Requires buf
import os
import shutil

# Change to substrait folder
sub_folder  = os.path.join(os.path.dirname(os.path.realpath(__file__)),'..','third_party','substrait')
os.chdir(sub_folder)


# Delete Current CPP files
shutil.rmtree(os.path.join(sub_folder,'substrait'))

# Clone Proto Files
os.system("git clone https://github.com/substrait-io/substrait git-sub")
git_folder =  os.path.join(sub_folder,'git-sub')

# Generate Proto Files
os.chdir(git_folder)
os.system('buf generate')

# Move Generated CPP files
shutil.move(os.path.join(git_folder,'gen','proto', 'cpp', 'substrait'), sub_folder)

# Delete Git Folder
shutil.rmtree(git_folder)
