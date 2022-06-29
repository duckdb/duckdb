---
name: Bug report
about: Create a report to help us improve
title: ''
labels: ''
assignees: ''

---

#### What happens?
A short, clear and concise description of what the bug is.

#### To Reproduce
Steps to reproduce the behavior. Bonus points if those are only SQL queries.

#### Environment (please complete the following information):
 - OS: (e.g. iOS)
 - DuckDB Version: [e.g. 22]
 - DuckDB Client: [e.g. Python]

#### Identity Disclosure:
 - Full Name: [e.g. John Doe]
 - Affiliation: [e.g. Oracle]

If the above is not given and is not obvious from your GitHub profile page, we might close your issue without further review. Please refer to the [reasoning behind this rule](https://berthub.eu/articles/posts/anonymous-help/) if you have questions.


#### Before Submitting

- [ ] **Have you tried this on the latest `master` branch?**
* **Python**: `pip install duckdb --upgrade --pre`
* **R**: `install.packages("https://github.com/duckdb/duckdb/releases/download/master-builds/duckdb_r_src.tar.gz", repos = NULL)`
* **Other Platforms**: You can find binaries [here](https://github.com/duckdb/duckdb/releases/tag/master-builds) or compile from source.

- [ ] **Have you tried the steps to reproduce? Do they include all relevant data and configuration? Does the issue you report still appear there?**
