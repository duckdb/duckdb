# deletes old dev wheels from pypi. evil hack.

pypi_username = "hfmuehleisen"

# gah
import ssl

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

import urllib.request
import json

url = 'https://pypi.python.org/pypi/duckdb/json'
req = urllib.request.urlopen(url, context=ctx)
raw_resp = req.read().decode()
resp_json =  json.loads(raw_resp)

last_release = resp_json["info"]["version"]

import distutils.version as version
latest_release_v = version.LooseVersion(last_release)

to_delete = []

# todo ugly double loop
latest_prerelease = -1

def parsever(ele):
	major = version.LooseVersion('.'.join(ele.split('.')[:3]))
	dev = int(ele.split('.')[3].replace('dev',''))
	return (major, dev,)

for ele in resp_json["releases"]:
	if not ".dev" in ele:
		continue

	(major, dev) = parsever(ele)

	if (major > latest_release_v and dev > latest_prerelease):
		latest_prerelease = dev


for ele in resp_json["releases"]:
	# never delete regular release builds
	if not ".dev" in ele:
		continue

	(major, dev) = parsever(ele)

	if (major <= latest_release_v or (major > latest_release_v and dev < latest_prerelease)):
		to_delete += [ele]


if (len(to_delete) < 1):
	raise ValueError("Nothing to do")

import os
pypi_password = os.getenv("PYPI_PASSWORD", "")
if pypi_password == "":
	raise ValueError('need Hannes\' PyPI password in PYPI_PASSWORD')


# gah2
import http.cookiejar
cj = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj), urllib.request.HTTPSHandler(context=ctx, debuglevel = 0))

def call(url, data=None, headers={}):
	return opener.open(urllib.request.Request(url, data, headers)).read().decode()


import re
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

	delete_data = urllib.parse.urlencode({
		"confirm_delete_version" : rev,
		"csrf_token" : delete_crsf_token
	}).encode()
	call("https://pypi.org/manage/project/duckdb/release/%s/" % rev, delete_data, delete_headers)


