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

fuzzer_desc = '''Issue found by ${FUZZER} on git commit hash [${SHORT_HASH}](https://github.com/duckdb/duckdb/commit/${FULL_HASH}) using seed ${SEED}.
'''

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
    if len(title) > 240:
        #  avoid title is too long error (maximum is 256 characters)
        title = title[:240] + '...'
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

def get_github_issues(page):
    session = create_session()
    url = issue_url()+'?per_page=100&page='+str(page)
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

def label_github_issue(number, label):
    session = create_session()
    url = issue_url() + '/' + str(number)
    params = {'labels': [label]}
    r = session.patch(url, json.dumps(params))
    if r.status_code == 200:
        print(f'Successfully labeled Issue "{number}"')
    else:
        print(f'Could not label Issue "{number}" (status code {r.status_code})')
        print('Response:', r.content.decode('utf8'))
        raise Exception("Failed to label issue")

def extract_issue(body, nr):
    try:
        splits = body.split(middle)
        sql = splits[0].split(header)[1]
        error = splits[1][:-len(footer)]
        return (sql, error)
    except:
        print(f"Failed to extract SQL/error message from issue {nr}")
        print(body)
        return None

def run_shell_command_batch(shell, cmd):
    command = [shell, '--batch', '-init', '/dev/null']

    try:
        res = subprocess.run(command, input=bytearray(cmd, 'utf8'), stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=300)
    except subprocess.TimeoutExpired:
        print(f"TIMEOUT... {cmd}")
        return ("", "", 0, True)
    stdout = res.stdout.decode('utf8').strip()
    stderr = res.stderr.decode('utf8').strip()
    return (stdout, stderr, res.returncode, False)

def test_reproducibility(shell, issue, current_errors, perform_check):
    extract = extract_issue(issue['body'], issue['number'])
    labels = issue['labels']
    label_timeout = False
    for label in labels:
        if label['name'] == 'timeout':
            label_timeout = True
    if extract is None:
        # failed extract: leave the issue as-is
        return True
    sql = extract[0] + ';'
    error = extract[1]
    if perform_check is True and label_timeout is False:
        print(f"Checking issue {issue['number']}...")
        (stdout, stderr, returncode, is_timeout) = run_shell_command_batch(shell, sql)
        if is_timeout:
            label_github_issue(issue['number'], 'timeout')
        else:
            if returncode == 0:
                return False
            if not fuzzer_helper.is_internal_error(stderr):
                return False
    # issue is still reproducible
    current_errors[error] = issue
    return True

def extract_github_issues(shell, perform_check):
    current_errors = dict()
    for p in range(1,10):
        issues = get_github_issues(p)
        for issue in issues:
            # check if the github issue is still reproducible
            if not test_reproducibility(shell, issue, current_errors, perform_check):
                # the issue appears to be fixed - close the issue
                print(f"Failed to reproduce issue {issue['number']}, closing...")
                close_github_issue(int(issue['number']))
    return current_errors

def file_issue(cmd, error_msg, fuzzer, seed, hash):
    # issue is new, file it
    print("Filing new issue to Github")

    title = error_msg
    body = fuzzer_desc.replace("${FUZZER}", fuzzer).replace("${FULL_HASH}", hash).replace("${SHORT_HASH}", hash[:5]).replace("${SEED}", str(seed))
    body += header + cmd + middle + error_msg + footer
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
