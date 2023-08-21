# R Package Release Process

## Preflight Checks
Check the GitHub actions reports for the revision you want to release.
[Github Actions R Workflow](https://github.com/duckdb/duckdb/actions/workflows/R.yml)
The output has a "R CMD check" section. Any errors/warnigns here are show stoppers and need to be addressed. NOTEs about package size or the "
Note to CRAN Maintainers" are fine, other NOTEs likely indicate problems. 

Also, have a look at the [CRAN package checks](https://cran.r-project.org/web/checks/check_results_duckdb.html). NOTEs or WARNINGs or anything in red needs to be addressed. Again, the NOTE about the package size is fine.

## Reverse Dependency Checks
Next you should run the reverse dependency check, checking if packages on CRAN that depend on DuckDB still work, any issues there will lead to issues with CRAN updates. The policy there is that you should contact package authors "well beforehand". If things fail in this check, you should contact the package authors.
````R
remotes::install_github("r-lib/revdepcheck") # once
revdepcheck::revdep_check(num_workers = 8, env=c(`MAKEVARS`="-j8"))
````

## Source Package
The R package does not automatically pick up the DuckDB version from the git tag. Make sure the file `tools/rpkg/DESCRIPTION` in the master branch has the `Version` entry that *you want to create*. 

It's important that package builds are created from the main branch of the duckdb/duckdb repo (not of a fork) because otherwise the git revision ids used to install extensions are wrong.

You can get the R source tarball either from the build artifacts (r-package-source) or from a GitHub release after CI finishes (`duckdb_[version].tar.gz`).

Next check the package using WinBuilder, upload the source tarball at https://win-builder.r-project.org/upload.aspx, using R-devel. You will get an email at some point. Again, warnings/notes besides the ones mentioned are bad.


## Upload
Finally, upload the package to CRAN here: https://cran.r-project.org/submit.html

The maintainer (currently Hannes) has to confirm the upload, after which various automatic checks are triggered.


## Post-release
By convention, development versions have a fourth component `.9000`. After a successful release, edit `tools/rpkg/DESCRIPTION` to include this, e.g., change `0.8.1` to `0.8.1.9000`.
