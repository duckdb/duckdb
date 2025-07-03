import github
import json # to write into a GITHUB_OUTPUT 
import os 
import pathspec # for git wildmatch
import subprocess
import yaml # because it much more readable than json

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

# on 'push' event we want get diff of previous(before) and current(after) commits in current branch
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
        print("Unknown event: Returning empty list.\nDon not trigger workflow runs.")
        return []

'''
filters beginning with '!' must not being ignored = include patterns
filters NOT beginning with '!' are ignored = exclude patterns 
'''
def should_run_workflow(changed_files, filters):
    patterns = filters.get('paths-ignore', [])

    include_patterns = [p[1:] for p in patterns if p.startswith('!')]
    exclude_patterns = [p for p in patterns if not p.startswith('!')]
    # preparing lists of exclude and include paths
    exclude_spec = pathspec.PathSpec.from_lines('gitwildmatch', exclude_patterns)
    include_spec = pathspec.PathSpec.from_lines('gitwildmatch', include_patterns)
    # run file_path through the lists of excluded paths and included paths
    # if at least one file found being NOT excluded or included, immediately return True to run the workflow
    for file_path in changed_files:
        print("FILE PATH:", file_path)
        if not exclude_spec.match_file(file_path):
            return True
        if include_spec.match_file(file_path):
            return True

    # if didn't hit to any of filters => don't run the workflow
    return False



result_config = {}
changed_files = get_changed_files()
for workflow, filters in config['workflows'].items():
    print("WORKFLOW: ", workflow)
    print("FILTERS: ", filters)
    changed_files = ['tools/sqlite3_api_wrapper/include/duckdb_shell_wrapper.h', '.github/workflows/Julia.yml', 'README.md', '.github/workflows/Blabla.yml']
    result = should_run_workflow(changed_files, filters)
    result_config[workflow] = result

print("Files changed:", changed_files)
print("Should run:", result_config)

# write result config to the GH Output
with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
    f.write(f"should_run={json.dumps(result_config)}\n")