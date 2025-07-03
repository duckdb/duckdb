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
    repo = os.environ["GITHUB_REPOSITORY"]
    pr_number = os.environ["PR_NUMBER"] if "PR_NUMBER" in os.environ else None
    token = os.environ["GITHUB_TOKEN"]
    # gh_pr_diff = ['gh', 'pr', 'diff', '18132', '--name-only']
    gh_pr_diff = ['gh', 'pr', 'diff', str(pr_number), '--name-only']
    result = subprocess.run(gh_pr_diff, text=True, check=True, capture_output=True)
    files = result.stdout.splitlines()
    return files


def should_run_workflow(changed_files, paths_ignore):
    for file in changed_files:
        if not any(fnmatch.fnmatch(file, path) for path in paths_ignore):
            return True
    return False

result_config = {}
changed_files = get_changed_files_pr()
for workflow, filters in config['workflows'].items():
    result = should_run_workflow(changed_files, filters.get('paths-ignore', []))
    result_config[workflow] = result

# write result config to the GH Output
with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
    f.write(f"should_run={json.dumps(result_config)}\n")