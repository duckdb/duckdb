import json
import requests
import sys
import os
import subprocess
import reduce_sql

if 'FUZZEROFDUCKSKEY' not in os.environ:
    print("FUZZEROFDUCKSKEY not found in environment variables")
    exit(1)

USERNAME = 'fuzzerofducks'
TOKEN = os.environ['FUZZEROFDUCKSKEY']

if len(TOKEN) == 0:
    print("FUZZEROFDUCKSKEY is set but is empty")
    exit(1)

if len(TOKEN) != 40:
    print("Incorrect length for FUZZEROFDUCKSKEY")
    exit(1)

REPO_OWNER = 'duckdb'
REPO_NAME = 'duckdb-fuzzer'

seed = -1

header = '''### To Reproduce
```sql
'''

middle = '''```

### Error Message
```
'''

footer = '''
```'''

fuzzer = None
db = None
shell = None
for param in sys.argv:
    if param == '--sqlsmith':
        fuzzer = 'sqlsmith'
    elif param == '--alltypes':
        db = 'alltypes'
    elif param == '--tpch':
        db = 'tpch'
    elif param.startswith('--shell='):
        shell = param.replace('--shell=', '')

if fuzzer is None:
    print("Unrecognized fuzzer to run, expected e.g. --sqlsmith")
    exit(1)

if db is None:
    print("Unrecognized database to run on, expected either --tpch or --alltypes")
    exit(1)

if shell is None:
    print("Unrecognized path to shell, expected e.g. --shell=build/debug/duckdb")
    exit(1)

# github stuff
def issue_url():
    return 'https://api.github.com/repos/%s/%s/issues' % (REPO_OWNER, REPO_NAME)

def create_session():
    # Create an authenticated session to create the issue
    session = requests.Session()
    session.headers.update({'Authorization': 'token %s' % (TOKEN,)})
    return session

def make_github_issue(title, body):
    session = create_session()
    url = issue_url()
    issue = {'title': title,
             'body': body}
    r = session.post(url, json.dumps(issue))
    if r.status_code == 201:
        print('Successfully created Issue "%s"' % title)
    else:
        print('Could not create Issue "%s"' % title)
        print('Response:', r.content.decode('utf8'))
        raise Exception("Failed to create issue")

def get_github_issues():
    session = create_session()
    url = issue_url()
    r = session.get(url)
    if r.status_code != 200:
        print('Failed to get list of issues')
        print('Response:', r.content.decode('utf8'))
        raise Exception("Failed to get list of issues")
    return json.loads(r.content.decode('utf8'))

def close_github_issue(number):
    session = create_session()
    url = issue_url() + '/' + str(number)
    params = {'state': 'closed'}
    r = session.patch(url, json.dumps(params))
    if r.status_code == 200:
        print(f'Successfully closed Issue "{number}"')
    else:
        print(f'Could not close Issue "{number}" (status code {r.status_code})')
        print('Response:', r.content.decode('utf8'))
        raise Exception("Failed to close issue")

def create_db_script(db):
    if db == 'alltypes':
        return 'create table all_types as select * exclude(small_enum, medium_enum, large_enum) from test_all_types();'
    elif db == 'tpch':
        return 'call dbgen(sf=0.1);'
    else:
        raise Exception("Unknown database creation script")

def run_fuzzer_script(fuzzer):
    if fuzzer == 'sqlsmith':
        return "call sqlsmith(max_queries=${MAX_QUERIES}, seed=${SEED}, verbose_output=1, log='${LAST_LOG_FILE}', complete_log='${COMPLETE_LOG_FILE}');"
    else:
        raise Exception("Unknown fuzzer type")

def run_shell_command(cmd):
    command = [shell, '--batch', '-init', '/dev/null']

    res = subprocess.run(command, input=bytearray(cmd, 'utf8'), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout = res.stdout.decode('utf8').strip()
    stderr = res.stderr.decode('utf8').strip()
    return (stdout, stderr, res.returncode)

def extract_issue(body, nr):
    try:
        splits = body.split(middle)
        sql = splits[0][len(header):]
        error = splits[1][:-len(footer)]
        return (sql, error)
    except:
        print(f"Failed to extract SQL/error message from issue {nr}")
        print(body)
        return None

def is_internal_error(error):
    if 'differs from original result' in error:
        return True
    if 'INTERNAL' in error:
        return True
    if 'signed integer overflow' in error:
        return True
    if 'Sanitizer' in error or 'sanitizer' in error:
        return True
    if 'runtime error' in error:
        return True
    return False

def test_reproducibility(issue, current_errors):
    extract = extract_issue(issue['body'], issue['number'])
    if extract is None:
        # failed extract: leave the issue as-is
        return True
    sql = extract[0] + ';'
    error = extract[1]
    (stdout, stderr, returncode) = run_shell_command(sql)
    if returncode == 0:
        return False
    if not is_internal_error(stderr):
        return False
    # issue is still reproducible
    current_errors[error] = issue
    return True

# first get a list of all github issues, and check if we can still reproduce them
current_errors = dict()

issues = get_github_issues()
for issue in issues:
    # check if the github issue is still reproducible
    if not test_reproducibility(issue, current_errors):
        # the issue appears to be fixed - close the issue
        print(f"Failed to reproduce issue {issue['number']}, closing...")
        close_github_issue(int(issue['number']))


max_queries = 1000
last_query_log_file = 'sqlsmith.log'
complete_log_file = 'sqlsmith.complete.log'

print(f'''==========================================
        RUNNING {fuzzer} on {db}
==========================================''')

load_script = create_db_script(db)
fuzzer = run_fuzzer_script(fuzzer).replace('${MAX_QUERIES}', str(max_queries)).replace('${LAST_LOG_FILE}', last_query_log_file).replace('${COMPLETE_LOG_FILE}', complete_log_file).replace('${SEED}', str(seed))

print(load_script)
print(fuzzer)

cmd = load_script + "\n" + fuzzer

print("==========================================")

(stdout, stderr, returncode) = run_shell_command(cmd)

print(f'''==========================================
        FINISHED RUNNING
==========================================''')
print("==============  STDOUT  ================")
print(stdout)
print("==============  STDERR  =================")
print(stderr)
print("==========================================")

print(returncode)
if returncode == 0:
    print("==============  SUCCESS  ================")
    exit(0)

print("==============  FAILURE  ================")
print("Attempting to reproduce and file issue...")

# run the last query, and see if the issue persists
with open(last_query_log_file, 'r') as f:
    last_query = f.read()

cmd = load_script + '\n' + last_query

(stdout, stderr, returncode) = run_shell_command(cmd)
if returncode == 0:
    print("Failed to reproduce the issue with a single command...")
    exit(0)

print("==============  STDOUT  ================")
print(stdout)
print("==============  STDERR  =================")
print(stderr)
print("==========================================")
if not is_internal_error(stderr):
    print("Failed to reproduce the internal error with a single command")
    exit(0)

error_msg = reduce_sql.sanitize_error(stderr)


print("=========================================")
print("         Reproduced successfully         ")
print("=========================================")

# check if this is a duplicate issue
if error_msg in current_errors:
    print("Skip filing duplicate issue")
    print("Issue already exists: https://github.com/duckdb/duckdb-fuzzer/issues/" + str(current_errors[error_msg]['number']))
    exit(0)

print(last_query)

print("=========================================")
print("        Attempting to reduce query       ")
print("=========================================")

# try to reduce the query as much as possible
last_query = reduce_sql.reduce(last_query, load_script, shell, error_msg)
cmd = load_script + '\n' + last_query + "\n"

# issue is new, file it
print("Filing new issue to Github")

title = error_msg
body = header + cmd + middle + error_msg + footer
print(title, body)
make_github_issue(title, body)
