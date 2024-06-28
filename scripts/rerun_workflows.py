import subprocess
import duckdb
import os
import pandas as pd
import argparse
from io import StringIO

parser = argparse.ArgumentParser(description='Rerun failed workflows from a PR.')
parser.add_argument(
    '--title',
    dest='title',
    action='store',
    help='The title of the PR for which we want to rerun workflows (or part of the title)',
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
        'displayTitle,databaseId,status,conclusion,headSha',
        f'--limit={nlimit}',
    ],
    stdout=subprocess.PIPE,
)
text = proc.stdout.read().decode('utf8')
df = pd.read_json(StringIO(text))
result = duckdb.query(f"select headSha from df where displayTitle LIKE '%{query}%' limit 1").fetchall()
if len(result) == 0:
    print(
        f"No workflows found in the latest {nlimit} workflows that contain the text {query}.\nPerhaps try running with a higher --max_workflows parameter?"
    )
    exit(1)

headSha = result[0][0]

result = duckdb.query(
    f"select databaseId from df where conclusion IN ('failure', 'cancelled') AND displayTitle LIKE '%{query}%' and headSha='{headSha}'"
).fetchall()
if len(result) == 0:
    print(f"Found runs that match the text {query} but no failing or cancelled runs were found")
for databaseId in [x[0] for x in result]:
    os.system(f'gh run -R {args.repo} rerun {databaseId}')
