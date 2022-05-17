import ssl
import urllib.request
import json
import os
import distutils.version as version
import http.cookiejar
import re

# deletes old dev wheels from pypi. evil hack.
retain_count = 3

pypi_username = "hfmuehleisen"
pypi_password = os.getenv("PYPI_PASSWORD", "")
if pypi_password == "":
	print(f'need {pypi_username}\' PyPI password in PYPI_PASSWORD env variable')
	exit(1)

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

url = 'https://pypi.python.org/pypi/duckdb/json'
req = urllib.request.urlopen(url, context=ctx)
raw_resp = req.read().decode()
resp_json =  json.loads(raw_resp)

last_release = resp_json["info"]["version"]
latest_release_v = version.LooseVersion(last_release)

latest_prereleases = []

def parsever(ele):
	major = version.LooseVersion('.'.join(ele.split('.')[:3]))
	dev = int(ele.split('.')[3].replace('dev',''))
	return (major, dev,)

# get a list of all pre-releases
release_list = resp_json["releases"]
for ele in release_list:
	if not ".dev" in ele:
		continue

	(major, dev) = parsever(ele)

	if major > latest_release_v:
		latest_prereleases.append((ele, dev))

# sort the pre-releases
latest_prereleases = sorted(latest_prereleases, key=lambda x: x[1])
print("List of pre-releases")
for prerelease in latest_prereleases:
	print(prerelease[0])

if len(latest_prereleases) <= retain_count:
	print(f"At most {retain_count} pre-releases - nothing to delete")
	exit(0)

to_delete = latest_prereleases[:len(latest_prereleases) - retain_count]
if len(to_delete) < 1:
	raise ValueError("Nothing to delete")

print("List of to-be-deleted releases")
for release in to_delete:
	print(release[0])

to_delete = [x[0] for x in to_delete]
print(to_delete)

# gah2
cj = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj), urllib.request.HTTPSHandler(context=ctx, debuglevel = 0))

def call(url, data=None, headers={}):
	return opener.open(urllib.request.Request(url, data, headers)).read().decode()


csrf_token_re = re.compile(r"name=\"csrf_token\"[^>]+value=\"([^\"]+)\"")

def get_token(url):
	return csrf_token_re.search(call(url)).group(1)


login_data = urllib.parse.urlencode({
	"csrf_token" : get_token("https://pypi.org/account/login/"),
	"username" : pypi_username,
	"password" : pypi_password}).encode()
login_headers = {
	"Referer": "https://pypi.org/account/login/"}

# perform login
call("https://pypi.org/account/login/", login_data, login_headers)

# delete gunk
delete_crsf_token = get_token("https://pypi.org/manage/project/duckdb/releases/")
delete_headers = {
	"Referer": "https://pypi.org/manage/project/duckdb/releases/"}

for rev in to_delete:
	print("Deleting %s" % rev)

	try:
		delete_data = urllib.parse.urlencode({
			"confirm_delete_version" : rev,
			"csrf_token" : delete_crsf_token
		}).encode()
		call("https://pypi.org/manage/project/duckdb/release/%s/" % rev, delete_data, delete_headers)
	except Exception as e:
		print(f"Failed to delete {rev}")
		print(e)
