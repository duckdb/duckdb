import subprocess
import duckdb
import os
import pandas as pd
import argparse
from io import StringIO


parser = argparse.ArgumentParser(description='Cancel all workflows related to a PR.')
parser.add_argument(
    '--title',
    dest='title',
    action='store',
    help='The title of the PR for which we want to rerun workflows (or part of the title) - or "master" for all pushes',
    required=True,
)
parser.add_argument(
    '--repo', dest='repo', action='store', help='The repository to run this workflow on', default='duckdb/duckdb'
)
parser.add_argument(
    '--max_workflows',
    dest='max_workflows',
    action='store',
    help='The maximum number of workflows to look at (starting from the latest)',
    default=200,
)
args = parser.parse_args()

nlimit = args.max_workflows
query = args.title


proc = subprocess.Popen(
    [
        'gh',
        'run',
        '-R',
        args.repo,
        'list',
        '--json',
        'displayTitle,databaseId,status,conclusion,headSha,event',
        f'--limit={nlimit}',
    ],
    stdout=subprocess.PIPE,
)
text = proc.stdout.read().decode('utf8')
df = pd.read_json(StringIO(text))

if query == 'master':
    result = duckdb.query(
        f"select databaseId from df WHERE status IN ('queued', 'in_progress') AND event='push'"
    ).fetchall()
else:
    result = duckdb.query(
        f"select databaseId from df WHERE status IN ('queued', 'in_progress') AND displayTitle LIKE '%{query}%'"
    ).fetchall()
if len(result) == 0:
    print(
        f"No workflows found in the latest {nlimit} workflows that contain the text {query}.\nPerhaps try running with a higher --max_workflows parameter?"
    )
    exit(1)
for databaseId in [x[0] for x in result]:
    os.system(f'gh run -R {args.repo} cancel {databaseId}')
