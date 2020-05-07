import json, os, sys, glob, mimetypes, urllib.request

api_url = 'https://api.github.com/repos/cwida/duckdb/'
tag = 'master-builds'

if (len(sys.argv) < 2):
	print("Usage: [filename1] [filename2] ... ")
	exit(1)

# this essentially should run on release tag builds to fill up release assets and master
# so check travis tag?

# TODO check TRAVIS_TAG

branch = os.getenv("TRAVIS_BRANCH", "")
if branch != "master":
	print("Only running on master branch for now. Exiting.")
	exit(0)

pr = os.getenv("TRAVIS_PULL_REQUEST", "")
if pr != "false":
	print("Not running on PRs. Exiting.")
	exit(0)

# sha = os.getenv("TRAVIS_COMMIT", "")
# if sha == "":
# 	raise ValueError('need a commit ID in TRAVIS_COMMIT')
sha = 'ba75d81601913782d28a3878707d135319f38bdd'

token = os.getenv("GH_TOKEN", "")
if token == "":
	raise ValueError('need a GitHub token in GH_TOKEN')

def gh_api(suburl, payload={}, filename='', method='GET'):
	url = api_url + suburl
	headers = {
		"Content-Type": "application/json",
		'Authorization': 'token ' + token
	}

	body_data = b''
	if len(payload) > 0:
		method = 'POST'
		body_data = json.dumps(payload).encode("utf-8")
		headers["Content-Length"] = len(body_data)

	if len(filename) > 0:
		method = 'POST'
		body_data = open(filename, 'rb')

		mime_type = mimetypes.guess_type(local_filename)[0]
		if mime_type is None:
			mime_type = "application/octet-stream"
		headers["Content-Type"] = mime_type
		headers["Content-Length"] = os.path.getsize(local_filename)

		url = suburl # cough

	req = urllib.request.Request(url, body_data, headers)
	req.get_method = lambda: method
	try:
		raw_resp = urllib.request.urlopen(req).read().decode()
	except urllib.error.HTTPError as e:
		raw_resp = e.read().decode() # gah

	if (method != 'DELETE'):
		return json.loads(raw_resp)
	else:
		return {}


# find out if commit exists in the first place
if 'sha' not in gh_api('commits/%s' % sha):
	raise ValueError('commit %s not found' % sha)

# check if tag exists with the correct hash already, if not, recreate tag & release
resp = gh_api('git/ref/tags/%s' % tag)
if 'object' not in resp or 'sha' not in resp['object'] : # or resp['object']['sha'] != sha
	print("re-creating tag & release")
	# clean up, delete release if exists and tag
	# this creates a potential race condition, use travis stages?
	resp = gh_api('releases/tags/%s' % tag)
	if "id" in resp:
		gh_api('releases/%s' % resp["id"], method='DELETE')

	gh_api('git/refs/tags/%s' % tag, method='DELETE')

	payload = {
	  "tag_name": tag,
	  "target_commitish": sha, # what a wonderful key name
	  "name": "Development builds from `master`",
	  "body": "This release contains builds for the latest commit to the master branch. They are meant for testing.",
	  "draft": False,
	  "prerelease": True
	}
	resp = gh_api('releases', payload=payload)
	print(resp)

resp = gh_api('releases/tags/%s' % tag)
if 'id' not in resp or 'upload_url' not in resp:
	raise ValueError('release does not exist for tag ' % tag)

# double-check that release exists and has correct sha
# disabled to not spam people watching releases
# if 'id' not in resp or 'upload_url' not in resp or 'target_commitish' not in resp or resp['target_commitish'] != sha:
# 	raise ValueError('release does not point to requested commit %s' % sha)

# TODO this could be a paged response!
assets = gh_api('releases/%s/assets' % resp['id'])

upload_url = resp['upload_url'].split('{')[0] # gah
files = sys.argv[1:]
for filename in files:
	if '=' in filename:
		parts = filename.split("=")
		asset_filename = parts[0]
		paths = glob.glob(parts[1])
		if len(paths) != 1:
			raise ValueError("Could not find file for pattern %s" % local_filename)
		local_filename = paths[0]
	else :
		asset_filename = os.path.basename(filename)
		local_filename = filename

	# delete if present
	for asset in assets:
		if asset['name'] == asset_filename:
			gh_api('releases/assets/%s' % asset['id'], method='DELETE')

	resp = gh_api(upload_url + '?name=%s' % asset_filename, filename=local_filename)
	if 'id' not in resp:
		raise ValueError('upload failed :/ ' + str(resp))
	print("%s -> %s" % (local_filename, resp['browser_download_url']))
