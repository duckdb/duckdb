import github
import yaml
import json
import fnmatch
import os 
import subprocess

# load paths to ignore
with open('.github/ci_filters.yaml', 'r') as f:
    config = yaml.safe_load(f)

def get_paths_ignore_for(workflow):
    return config['workflows'][workflow]['paths-ignore']

# check updated files in PR
def get_changed_files_pr():
    pr_number = os.environ["PR_NUMBER"] if "PR_NUMBER" in os.environ else None
    # gh_pr_diff = ['gh', 'pr', 'diff', '18132', '--name-only']
    gh_pr_diff = ['gh', 'pr', 'diff', str(pr_number), '--name-only']
    result = subprocess.run(gh_pr_diff, text=True, check=True, capture_output=True)
    files = result.stdout.splitlines()
    return files

def get_changed_files_push():
    before = os.environ.get('GITHUB_EVENT_BEFORE')
    after = os.environ.get('GITHUB_SHA')
    # before = 'fb97862'
    # after = 'ad80307'

    files = []
    if before and after:
        subprocess.run(['git', 'fetch', '--no-tags', '--depth=100', 'origin', os.environ['GITHUB_REF']], check=True)
        # subprocess.run(['git', 'fetch', '--no-tags', '--depth=100', 'origin', 'refs/heads/main'], check=True)
        result = subprocess.check_output(['git', 'diff', '--name-only', before, after])
        files = result.decode().splitlines()
    return files


def should_run_workflow(changed_files, paths_ignore):
    for file in changed_files:
        if not any(fnmatch.fnmatch(file, path) for path in paths_ignore):
            return True
    return False

def get_changed_files():
    event_name = os.getenv["GITHIB_EVENT_NAME"]
    if event_name == "pull_request":
        return get_changed_files_pr()
    elif event_name == "push":
        return get_changed_files_push()
    else:
        print("Unknown event")
        return []

result_config = {}
changed_files = get_changed_files()
# print("🦑", changed_files)
for workflow, filters in config['workflows'].items():
    result = should_run_workflow(changed_files, filters.get('paths-ignore', []))
    result_config[workflow] = result

# write result config to the GH Output
with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
    f.write(f"should_run={json.dumps(result_config)}\n")