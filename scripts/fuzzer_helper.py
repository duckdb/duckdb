import json
import requests
import sys
import os
import subprocess
import reduce_sql
import fuzzer_helper

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

header = '''### To Reproduce
```sql
'''

middle = '''
```

### Error Message
```
'''

footer = '''
```'''

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

def run_shell_command_batch(shell, cmd):
    command = [shell, '--batch', '-init', '/dev/null']

    res = subprocess.run(command, input=bytearray(cmd, 'utf8'), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout = res.stdout.decode('utf8').strip()
    stderr = res.stderr.decode('utf8').strip()
    return (stdout, stderr, res.returncode)

def test_reproducibility(shell, issue, current_errors):
    extract = extract_issue(issue['body'], issue['number'])
    if extract is None:
        # failed extract: leave the issue as-is
        return True
    sql = extract[0] + ';'
    error = extract[1]
    (stdout, stderr, returncode) = run_shell_command_batch(shell, sql)
    if returncode == 0:
        return False
    if not fuzzer_helper.is_internal_error(stderr):
        return False
    # issue is still reproducible
    current_errors[error] = issue
    return True

def extract_github_issues(shell):
    current_errors = dict()
    issues = get_github_issues()
    for issue in issues:
        # check if the github issue is still reproducible
        if not test_reproducibility(shell, issue, current_errors):
            # the issue appears to be fixed - close the issue
            print(f"Failed to reproduce issue {issue['number']}, closing...")
            close_github_issue(int(issue['number']))
    return current_errors

def file_issue(cmd, error_msg):
    # issue is new, file it
    print("Filing new issue to Github")

    title = error_msg
    body = header + cmd + middle + error_msg + footer
    print(title, body)
    make_github_issue(title, body)

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
