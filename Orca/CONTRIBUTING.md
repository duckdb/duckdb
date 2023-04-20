# Contributing

We accept contributions via [Github Pull requests](https://help.github.com/articles/using-pull-requests) only.

Follow the steps below to open a PR:
1. Sign the [Contributor License Agreement](https://cla.pivotal.io/sign/greenplum).
2. Fork the project’s repository
3. Create your own feature branch (e.g. `git checkout -b better_orca`) and make changes on this branch.
    * Follow the instructions in the [README](README.md) file to set up and build in your environment.
4. Follow the naming and formatting style guide described [here](StyleGuide.md).
5. In some cases, ICG tests in GPDB may also need to be modified or additional ICG tests may need to be added to provide full coverage for the fix (for example, if the patch fixes wrong results or an execution specific issue). If such a situation occurs, please create a GPDB pull request and reference it in the GPORCA pull request.
6. Run through all the [tests](README.md#test) in your feature branch and ensure they are successful.
    * Follow the [Add tests](README.md#addtest) section to add new tests.
    * Follow the [Update tests](README.md#updatetest) section to update existing tests.
    * Make sure that ctest passes in both debug and retail build since there are some tests that do not overlap.
7. Push your local branch to your fork (e.g. `git push origin better_orca`) and [submit a pull request](https://help.github.com/articles/creating-a-pull-request)
8. Wait for people to review the PR and address comments by making additional commits and pushing them to your branch.
9. Before merging the PR, one more step is to bump the ORCA version: Bump the `GPORCA_VERSION_MINOR` in `CMakeLists.txt` whenever your changes affect the ORCA functionality. `GPORCA_VERSION_PATCH` is bumped only in case where
the changes do not affect ORCA functionality e.g. updating the `README.md`, adding a test case, fixing comments etc. Bumping the minor version requires a corresponding change in GPDB.
10. Merge the commit, using the "Squash and Merge" button on github. If you are not a committer, a committer will do this for you.

Your contribution will be analyzed for product fit and engineering quality prior to merging.
Note: All contributions must be sent using GitHub Pull Requests.

**Your pull request is much more likely to be accepted if it is small and focused with a clear message that conveys the intent of your change.**

Overall we follow GPDB's comprehensive contribution policy. Please refer to it [here](https://github.com/greenplum-db/gpdb#contributing) for details.
