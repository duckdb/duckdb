import os
import sys
from pathlib import Path
from tree import tree

class TestExamples(object):
    def test_examples(self):
        relative_paths = [
            '../../../../',
            ''
        ]
        # Debug output to get folder structure in CI
        for path in relative_paths:
            print("---" + path + "---")
            for line in tree(Path(path)):
                print(line)

        if 'CIBW_ROOT_DIR' in os.environ:
            # This is being called from the CI by cibuildhweel
            # the project+tests have been copied to another location, this variable contains the working dir where cibuildwheel was called from
            root_dir = os.getenv('CIBW_ROOT_DIR')
        else:
            root_dir = os.path.dirname(os.path.abspath(__file__))
        # Append the folder of the module to the paths
        paths = [
            os.path.join(path, 'examples/python') for path in relative_paths
        ]
        # Prepend the directory of the script to the paths
        paths = [
            os.path.abspath(os.path.join(root_dir, path)) for path in paths
        ]
        print(paths)
        sys.path.extend(paths)

        # Import the examples module, which subsequently runs
        import duckdb_python
