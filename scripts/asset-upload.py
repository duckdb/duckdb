import requests, json, os, sys, mimetypes

if (len(sys.argv) < 2):
    print("Usage: [filename1] [filename2] ... ")
    exit(1)

api_url = 'https://api.github.com/repos/cwida/duckdb/'

tag = 'master-builds'

# this essentially should run on release tag builds to fill up release assets and master
# so check travis tag?

# TODO check TRAVIS_TAG

token = os.getenv("GH_TOKEN", "")
if token == "":
	raise ValueError('need a GitHub token in GH_TOKEN')
header = {'Authorization': 'token ' + token}


branch = os.getenv("TRAVIS_BRANCH", "")
if branch != "ghassets":
	print("Only running on ghassets branch for now. Exiting.")
	exit(0)

sha = os.getenv("TRAVIS_COMMIT", "")
if sha == "":
	raise ValueError('need a commit ID in TRAVIS_COMMIT')

# find out if commit exists in the first place
resp = requests.get(api_url +'commits/%s' % sha, headers=header).json()
if 'sha' not in resp:
	raise ValueError('commit %s not found' % sha)

# check if tag exists with the correct hash already, if not, recreate tag & release
resp = requests.get(api_url +'git/ref/tags/%s' % tag, headers=header).json()
if 'object' not in resp or 'sha' not in resp['object'] or resp['object']['sha'] != sha:
	print("re-creating tag & release")
	# clean up, delete release if exists and tag
	# this creates a potential race condition, use travis stages?
	response_json = requests.get(api_url +'releases/tags/%s' % tag, headers=header).json()
	if "id" in response_json:
		requests.delete(api_url +'releases/%s' % response_json["id"], headers=header)
	requests.delete(api_url +'git/refs/tags/%s' % tag, headers=header)

	payload = {
	  "tag_name": tag,
	  "target_commitish": sha, # what a wonderful key name
	  "name": "Development builds from `master`",
	  "body": "This release contains builds for the latest commit to the master branch. They are meant for testing.",
	  "draft": False,
	  "prerelease": True
	}
	response_json = requests.post(api_url +'releases', json=payload, headers=header).json()

# double-check that release exists and has correct sha
resp = requests.get(api_url +'releases/tags/%s' % tag, headers=header).json()
if 'id' not in resp or 'upload_url' not in resp or 'target_commitish' not in resp or resp['target_commitish'] != sha:
	raise ValueError('release does not point to requested commit %s' % sha)

upload_url = resp['upload_url'].split('{')[0] # gah

files = sys.argv[1:]
for filename in files:
	if '=' in filename:
		parts = filename.split("=")
		local_filename = parts[1]
		asset_filename = parts[0]
	else :
		local_filename = filename
		asset_filename = os.path.basename(filename)

	mime_type = mimetypes.guess_type(local_filename)[0]
	if mime_type is None:
		mime_type = "application/octet-stream"

	header["Content-Type"] = mime_type
	with open(local_filename, 'rb') as f:
	    resp = requests.post(upload_url + '?name=%s' % asset_filename, data=f, headers=header).json()
	    if 'id' not in resp:
	    	raise ValueError('upload failed :/ ' + str(resp))
	    print(resp['browser_download_url'])
