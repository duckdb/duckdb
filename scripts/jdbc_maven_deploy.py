# https://central.sonatype.org/pages/manual-staging-bundle-creation-and-deployment.html
# https://issues.sonatype.org/browse/OSSRH-58179

# this is the pgp key we use to sign releases
# if this key should be lost, generate a new one with `gpg --full-generate-key`
# AND upload to keyserver: `gpg --keyserver hkp://keys.openpgp.org --send-keys [...]`
# export the keys for GitHub Actions like so: `gpg --export-secret-keys | base64`
# --------------------------------
# pub   ed25519 2022-02-07 [SC]
#       65F91213E069629F406F7CF27F610913E3A6F526
# uid           [ultimate] DuckDB <quack@duckdb.org>
# sub   cv25519 2022-02-07 [E]

import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import zipfile
import re


def exec(cmd):
    print(cmd)
    res = subprocess.run(cmd.split(' '), capture_output=True)
    if res.returncode == 0:
        return res.stdout
    raise ValueError(res.stdout + res.stderr)


if len(sys.argv) < 4 or not os.path.isdir(sys.argv[2]) or not os.path.isdir(sys.argv[3]):
    print("Usage: [release_tag, format: v1.2.3] [artifact_dir] [jdbc_root_path]")
    exit(1)

version_regex = re.compile(r'^v((\d+)\.(\d+)\.\d+)$')
release_tag = sys.argv[1]
deploy_url = 'https://oss.sonatype.org/service/local/staging/deploy/maven2/'
is_release = True

if release_tag == 'main':
    # for SNAPSHOT builds we increment the minor version and set patch level to zero.
    # seemed the most sensible
    last_tag = exec('git tag --sort=-committerdate').decode('utf8').split('\n')[0]
    re_result = version_regex.search(last_tag)
    if re_result is None:
        raise ValueError("Could not parse last tag %s" % last_tag)
    release_version = "%d.%d.0-SNAPSHOT" % (int(re_result.group(2)), int(re_result.group(3)) + 1)
    # orssh uses a different deploy url for snapshots yay
    deploy_url = 'https://oss.sonatype.org/content/repositories/snapshots/'
    is_release = False
elif version_regex.match(release_tag):
    release_version = version_regex.search(release_tag).group(1)
else:
    print("Not running on %s" % release_tag)
    exit(0)

jdbc_artifact_dir = sys.argv[2]
jdbc_root_path = sys.argv[3]

combine_builds = ['linux-amd64', 'osx-universal', 'windows-amd64', 'linux-aarch64']

staging_dir = tempfile.mkdtemp()

binary_jar = '%s/duckdb_jdbc-%s.jar' % (staging_dir, release_version)
pom = '%s/duckdb_jdbc-%s.pom' % (staging_dir, release_version)
sources_jar = '%s/duckdb_jdbc-%s-sources.jar' % (staging_dir, release_version)
javadoc_jar = '%s/duckdb_jdbc-%s-javadoc.jar' % (staging_dir, release_version)

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
      <url>https://raw.githubusercontent.com/duckdb/duckdb/main/LICENSE</url>
      <distribution>repo</distribution>
    </license>
  </licenses>

  <developers>
      <developer>
      <name>Mark Raasveldt</name>
      <email>mark@duckdblabs.com</email>
      <organization>DuckDB Labs</organization>
      <organizationUrl>https://www.duckdblabs.com</organizationUrl>
    </developer>
    <developer>
      <name>Hannes Muehleisen</name>
      <email>hannes@duckdblabs.com</email>
      <organization>DuckDB Labs</organization>
      <organizationUrl>https://www.duckdblabs.com</organizationUrl>
    </developer>
  </developers>

  <scm>
    <connection>scm:git:git://github.com/duckdb/duckdb.git</connection>
    <developerConnection>scm:git:ssh://github.com:duckdb/duckdb.git</developerConnection>
    <url>http://github.com/duckdb/duckdb/tree/main</url>
  </scm>

  <build>
    <plugins>
      <plugin>
        <groupId>org.sonatype.plugins</groupId>
        <artifactId>nexus-staging-maven-plugin</artifactId>
        <version>1.6.13</version>
        <extensions>true</extensions>
        <configuration>
         <serverId>ossrh</serverId>
         <nexusUrl>https://oss.sonatype.org/</nexusUrl>
       </configuration>
     </plugin>
   </plugins>
 </build>

</project>
<!-- Note: this cannot be used to build the JDBC driver, we only use it to deploy -->
"""

# create a matching POM with this version
pom_path = pathlib.Path(pom)
pom_path.write_text(pom_template.replace("${VERSION}", release_version))

# fatten up jar to add other binaries, start with first one
shutil.copyfile(os.path.join(jdbc_artifact_dir, "java-" + combine_builds[0], "duckdb_jdbc.jar"), binary_jar)
for build in combine_builds[1:]:
    old_jar = zipfile.ZipFile(os.path.join(jdbc_artifact_dir, "java-" + build, "duckdb_jdbc.jar"), 'r')
    for zip_entry in old_jar.namelist():
        if zip_entry.startswith('libduckdb_java.so'):
            old_jar.extract(zip_entry, staging_dir)
            exec("jar -uf %s -C %s %s" % (binary_jar, staging_dir, zip_entry))

javadoc_stage_dir = tempfile.mkdtemp()

exec("javadoc -Xdoclint:-reference -d %s -sourcepath %s/src/main/java org.duckdb" % (javadoc_stage_dir, jdbc_root_path))
exec("jar -cvf %s -C %s ." % (javadoc_jar, javadoc_stage_dir))
exec("jar -cvf %s -C %s/src/main/java org" % (sources_jar, jdbc_root_path))

# make sure all files exist before continuing
if (
    not os.path.exists(javadoc_jar)
    or not os.path.exists(sources_jar)
    or not os.path.exists(pom)
    or not os.path.exists(binary_jar)
):
    raise ValueError('could not create all required files')

# run basic tests, it should now work on whatever platform this is
exec("java -cp %s org.duckdb.test.TestDuckDBJDBC" % binary_jar)

# now sign and upload everything
# for this to work, you must have entry in ~/.m2/settings.xml:

# <settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
#   xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
#   xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
#   https://maven.apache.org/xsd/settings-1.0.0.xsd">
#   <servers>
#     <server>
#       <id>ossrh</id>
#       <username>hfmuehleisen</username> <!-- Sonatype OSSRH JIRA user/pw -->
#       <password>[...]</password>
#     </server>
#   </servers>
# </settings>

results_dir = os.path.join(jdbc_artifact_dir, "results")
if not os.path.exists(results_dir):
    os.mkdir(results_dir)


for jar in [binary_jar, sources_jar, javadoc_jar]:
    shutil.copyfile(jar, os.path.join(results_dir, os.path.basename(jar)))

print("JARs created, uploading (this can take a while!)")
deploy_cmd_prefix = 'mvn gpg:sign-and-deploy-file -Durl=%s -DrepositoryId=ossrh' % deploy_url
exec("%s -DpomFile=%s -Dfile=%s" % (deploy_cmd_prefix, pom, binary_jar))
exec("%s -Dclassifier=sources -DpomFile=%s -Dfile=%s" % (deploy_cmd_prefix, pom, sources_jar))
exec("%s -Dclassifier=javadoc -DpomFile=%s -Dfile=%s" % (deploy_cmd_prefix, pom, javadoc_jar))


if not is_release:
    print("Not a release, not closing repo")
    exit(0)

print("Close/Release steps")
# # beautiful
os.environ["MAVEN_OPTS"] = '--add-opens=java.base/java.util=ALL-UNNAMED'

# this list has horrid output, lets try to parse. What we want starts with orgduckdb- and then a number
repo_id = re.search(r'(orgduckdb-\d+)', exec("mvn -f %s nexus-staging:rc-list" % (pom)).decode('utf8')).groups()[0]
exec("mvn -f %s nexus-staging:rc-close -DstagingRepositoryId=%s" % (pom, repo_id))
exec("mvn -f %s nexus-staging:rc-release -DstagingRepositoryId=%s" % (pom, repo_id))

print("Done?")
