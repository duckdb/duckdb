import github
import yaml
import json
import fnmatch
import os 
import subprocess

ci = True if 'CI' in os.environ and os.getenv('CI') == 'true' else False

# load paths to ignore
with open('.github/ci_filters.yaml', 'r') as f:
    config = yaml.safe_load(f)

def get_paths_ignore_for(workflow):
    return config['workflows'][workflow]['paths-ignore']

# check updated files in PR
def get_changed_files_pr():
    pr_number = os.environ["PR_NUMBER"] if "PR_NUMBER" in os.environ else None
    gh_pr_diff = ['gh', 'pr', 'diff', str(pr_number), '--name-only'] if ci else ['gh', 'pr', 'diff', '18132', '--name-only']
    result = subprocess.run(gh_pr_diff, text=True, check=True, capture_output=True)
    files = result.stdout.splitlines()
    return files

def get_changed_files_push():
    previous_head = 'HEAD^1'
    current_head = 'HEAD'
    before = subprocess.check_output(['git', 'rev-parse', previous_head], text=True).strip()
    after = subprocess.check_output(['git', 'rev-parse', current_head], text=True).strip()

    files = []
    git_ref = os.environ['GITHUB_REF']
    print("Git Ref: ", git_ref, "Before: ", before, "After: ", after)
    if before and after:
        command = ['git', 'fetch', '--no-tags', '--depth=100', 'origin', git_ref] if ci else ['git', 'fetch', '--no-tags', '--depth=100', 'origin', 'refs/heads/main']
        subprocess.run(command, check=True)
        result = subprocess.check_output(['git', 'diff', '--name-only', before, after])
        files = result.decode().splitlines()
    return files

def get_changed_files():
    event_name = os.getenv("GITHUB_EVENT_NAME")
    print("Event name: ", event_name)
    if event_name == "pull_request":
        return get_changed_files_pr()
    elif event_name == "push":
        return get_changed_files_push()
    else:
        print("Unknown event")
        return []

# returns True to trigger runs when changed files not found in CI filters
def should_run_workflow(changed_files, paths_ignore):
    for file in changed_files:
        if not any(fnmatch.fnmatch(file, path) for path in paths_ignore):
            return True
    return False

result_config = {}
changed_files = get_changed_files()
# print("🦑", changed_files)
for workflow, filters in config['workflows'].items():
    result = should_run_workflow(changed_files, filters.get('paths-ignore', []))
    result_config[workflow] = result

print("Files changed:", changed_files)
print("Should run:", result_config)

# write result config to the GH Output
with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
    f.write(f"should_run={json.dumps(result_config)}\n")