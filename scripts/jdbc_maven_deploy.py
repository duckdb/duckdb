# https://central.sonatype.org/pages/manual-staging-bundle-creation-and-deployment.html
# https://issues.sonatype.org/browse/OSSRH-58179

# this is the pgp key we use to sign releases
# if this key should be lost, generate a new one with `gpg --full-generate-key` 
# AND upload to keyserver: `gpg --keyserver hkp://keys.openpgp.org --send-keys [...]`

# --------------------------------
# sec   rsa2048 2020-06-04 [SC]
#       63A86642934CDDC017123DE8B1DE1389E91BE914
# uid           [ultimate] DuckDB <quack@duckdb.org>
# ssb   rsa2048 2020-06-04 [E]

import glob
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import urllib.request
import zipfile
import re

version_regex = re.compile(r'^v(\d+\.\d+\.\d+)$')

if len(sys.argv) < 2 or not version_regex.match(sys.argv[1]):
	print("Usage: [release_tag, format: v1.2.3]")
	exit(1)

staging_dir = tempfile.mkdtemp()
release_tag = sys.argv[1]
release_version = version_regex.search(release_tag).group(1)

release_prefix = 'https://github.com/duckdb/duckdb/releases/download/%s' % release_tag

binary_jar = '%s/duckdb_jdbc-%s.jar' % (staging_dir, release_version)
pom = '%s/duckdb_jdbc-%s.pom' % (staging_dir, release_version)
sources_jar = '%s/duckdb_jdbc-%s-sources.jar' % (staging_dir, release_version)
javadoc_jar = '%s/duckdb_jdbc-%s-javadoc.jar' % (staging_dir, release_version)

combine_builds = ['linux-amd64', 'osx-universal', 'windows-amd64']
for build in combine_builds:
	file_url = '%s/duckdb_jdbc-%s.jar' % (release_prefix, build)
	# print(file_url)
	urllib.request.urlretrieve(file_url, '%s/duckdb_jdbc-%s.jar' % (staging_dir, build))

# fatten up jar to add other binaries, start with first one
shutil.copyfile('%s/duckdb_jdbc-%s.jar' % (staging_dir, combine_builds[0]), binary_jar)

def exec(cmd):
	subprocess.run(cmd.split(' '), check=True, stdout=subprocess.DEVNULL)

for build in combine_builds[1:]:
	old_jar = zipfile.ZipFile('%s/duckdb_jdbc-%s.jar' % (staging_dir, build), 'r')
	for zip_entry in old_jar.namelist():
		if zip_entry.startswith('libduckdb_java.so'):
			old_jar.extract(zip_entry, staging_dir)
			exec("jar -uf %s -C %s %s" % (binary_jar, staging_dir, zip_entry))

pom_template = """
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>org.duckdb</groupId>
  <artifactId>duckdb_jdbc</artifactId>
  <version>${VERSION}</version>
  <packaging>jar</packaging>
  <name>DuckDB JDBC Driver</name>
  <description>A JDBC-Compliant driver for the DuckDB data management system</description>
  <url>https://www.duckdb.org</url>

  <licenses>
    <license>
      <name>MIT License</name>
      <url>https://raw.githubusercontent.com/duckdb/duckdb/master/LICENSE</url>
      <distribution>repo</distribution>
    </license>
  </licenses>

  <developers>
    <developer>
      <name>Hannes Muehleisen</name>
      <email>hannes@cwi.nl</email>
      <organization>CWI</organization>
      <organizationUrl>https://www.cwi.nl</organizationUrl>
    </developer>
  </developers>

  <scm>
    <connection>scm:git:git://github.com/duckdb/duckdb.git</connection>
    <developerConnection>scm:git:ssh://github.com:duckdb/duckdb.git</developerConnection>
    <url>http://github.com/duckdb/duckdb/tree/master</url>
  </scm>
</project>
<!-- Note: this cannot be used to build the JDBC driver, we only use it to deploy -->
"""

# create a matching POM with this version
pom_path = pathlib.Path(pom)
pom_path.write_text(pom_template.replace("${VERSION}", release_version))

# download sources to create separate sources and javadoc JARs, this is required by maven central
source_zip_url = 'https://github.com/duckdb/duckdb/archive/%s.zip' % release_tag 
source_zip_file = tempfile.mkstemp()[1]
source_zip_dir = tempfile.mkdtemp()
# print(source_zip_url)
urllib.request.urlretrieve(source_zip_url, source_zip_file)
zipfile.ZipFile(source_zip_file, 'r').extractall(source_zip_dir)
jdbc_root_path = glob.glob('%s/*/tools/jdbc' % source_zip_dir)[0]
javadoc_stage_dir = tempfile.mkdtemp()

exec("javadoc -d %s -sourcepath %s/src/main/java org.duckdb" % (javadoc_stage_dir, jdbc_root_path))
exec("jar -cvf %s -C %s ." % (javadoc_jar, javadoc_stage_dir))
exec("jar -cvf %s -C %s/src/main/java org" % (sources_jar, jdbc_root_path))

# make sure all files exist before continuing
if not os.path.exists(javadoc_jar) or not os.path.exists(sources_jar) or not os.path.exists(pom) or not os.path.exists(binary_jar):
	raise ValueError('could not create all required files') 

# run basic tests, it should now work on whatever platform this is
exec("java -cp %s org.duckdb.test.TestDuckDBJDBC" % binary_jar)

# now sign and upload everything
# for this to work, you must have entry in ~/.m2/settings.xml:

# <settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
# 	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
# 	xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
# 	https://maven.apache.org/xsd/settings-1.0.0.xsd">
# 	<servers>
# 		<server>
# 			<id>ossrh</id>
# 			<username>hfmuehleisen</username> <!-- Sonatype OSSRH JIRA user/pw -->
# 			<password>[...]</password>
# 		</server>
# 	</servers>
# </settings>

#exit(0)

print("JARs created, uploading (this can take a while!). When done, visit https://oss.sonatype.org")
deploy_cmd_prefix = 'mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=ossrh'
exec("%s -DpomFile=%s -Dfile=%s" % (deploy_cmd_prefix, pom, binary_jar))
exec("%s -Dclassifier=sources -DpomFile=%s -Dfile=%s" % (deploy_cmd_prefix, pom, sources_jar))
exec("%s -Dclassifier=javadoc -DpomFile=%s -Dfile=%s" % (deploy_cmd_prefix, pom, javadoc_jar))

# manual step: login to https://oss.sonatype.org , login, go to "staging repositories", 'close' and then 'release'
# TODO upload the asset to gh releases, too!

