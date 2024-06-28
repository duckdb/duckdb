import json, os, sys, glob, mimetypes, urllib.request, re

api_url = 'https://api.github.com/repos/duckdb/duckdb/'

if len(sys.argv) < 2:
    print("Usage: [last_tag] ")
    exit(1)


token = os.getenv("GH_TOKEN", "")
if token == "":
    raise ValueError('need a GitHub token in GH_TOKEN')


# amazingly this is the entire code of the pypy package `linkheader-parser`
def extract(link_header):
    """Extract links and their relations from a Link Header Field."""
    links = [l.strip() for l in link_header.split(',')]
    rels = {}
    pattern = r'<(?P<url>.*)>;\s*rel="(?P<rel>.*)"'
    for link in links:
        group_dict = re.match(pattern, link).groupdict()
        rels[group_dict['rel']] = group_dict['url']
    return rels


def gh_api(suburl, full_url=''):
    if full_url == '':
        url = api_url + suburl
    else:
        url = full_url
    headers = {"Content-Type": "application/json", 'Authorization': 'token ' + token}

    req = urllib.request.Request(url, b'', headers)
    req.get_method = lambda: 'GET'
    next_link = None
    try:
        resp = urllib.request.urlopen(req)
        if not resp.getheader("Link") is None:
            link_data = extract(resp.getheader("Link"))
            if "next" in link_data:
                next_link = link_data["next"]
        raw_resp = resp.read().decode()
    except urllib.error.HTTPError as e:
        raw_resp = e.read().decode()  # gah

    ret_json = json.loads(raw_resp)
    if next_link is not None:
        return ret_json + gh_api('', full_url=next_link)
    return ret_json


# get time of tag
old_release = gh_api('releases/tags/%s' % sys.argv[1])
print(old_release["published_at"])

pulls = gh_api('pulls?base=main&state=closed')
for p in pulls:
    if p["merged_at"] is None:
        continue
    if p["merged_at"] < old_release["published_at"]:
        continue
    print(" - #%s: %s" % (p["number"], p["title"]))
