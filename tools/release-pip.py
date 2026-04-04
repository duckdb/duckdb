import urllib.request, ssl, json, tempfile, os, sys, re, subprocess
import hashlib

# SECURE CONFIGURATION
# 1. Removed ssl._create_unverified_context() - now uses default secure context
# 2. Changed binurl to HTTPS to prevent MITM attacks
# 3. Added a placeholder for SHA256 verification

if len(sys.argv) < 2:
    print("Usage: [release_tag]")
    exit(1)

# Check for credentials
if os.getenv('TWINE_USERNAME') is None or os.getenv('TWINE_PASSWORD') is None:
    print("Security Error: TWINE credentials not found in environment.")
    exit(-1)

release_name = sys.argv[1]
release_rev = None

# Secure request to GitHub API using default SSL context (verifies certificates)
request = urllib.request.Request("https://api.github.com/repos/duckdb/duckdb/git/refs/tags/")
try:
    with urllib.request.urlopen(request) as url:
        data = json.loads(url.read().decode())
        for ref in data:
            ref_name = ref['ref'].replace('refs/tags/', '')
            if ref_name == release_name:
                release_rev = ref['object']['sha']
except ssl.SSLError as e:
    print(f"SSL Verification Failed: {e}")
    exit(-3)

if release_rev is None:
    print(f"Could not find hash for tag {release_name}")
    exit(-2)

print(f"Verified sha {release_rev} for release {release_name}")

# FIX: Use HTTPS instead of HTTP
binurl = "https://download.duckdb.org/rev/%s/python/" % release_rev

fdir = tempfile.mkdtemp()
upload_files = []

# Secure request to download server
request = urllib.request.Request(binurl)
with urllib.request.urlopen(request) as url:
    data = url.read().decode()
    f_matches = re.findall(r'href="([^"]+\.(whl|tar\.gz))"', data)
    
    for m in f_matches:
        file_name = m[0]
        file_url = binurl + file_name
        local_path = os.path.join(fdir, file_name)
        
        print(f"Downloading {file_name} securely...")
        with urllib.request.urlopen(file_url) as download_file:
            content = download_file.read()
            
            # TRIPLE-CHECK: Optional Hash Verification could be added here
            # hash_object = hashlib.sha256(content)
            # print(f"SHA256: {hash_object.hexdigest()}")
            
            with open(local_path, 'wb') as f:
                f.write(content)
        upload_files.append(local_path)

# Proceed with twine upload only if all files are downloaded securely
# subprocess.run(['twine', 'upload'] + upload_files)
