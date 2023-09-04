# Contributing

## Code of Conduct

This project and everyone participating in it is governed by a [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to [quack@duckdb.org](mailto:quack@duckdb.org).


## **Did you find a bug?**

* **Ensure the bug was not already reported** by searching on GitHub under [Issues](https://github.com/duckdb/duckdb/issues).
* If you're unable to find an open issue addressing the problem, [open a new one](https://github.com/duckdb/duckdb/issues/new). Be sure to include a **title and clear description**, as much relevant information as possible, and a **code sample** or an **executable test case** demonstrating the expected behavior that is not occurring.

## **Did you write a patch that fixes a bug?**

* Great!
* If possible, add a unit test case to make sure the issue does not occur again.
* Make sure you run the code formatter (`make format-fix`).
* Open a new GitHub pull request with the patch.
* Ensure the PR description clearly describes the problem and solution. Include the relevant issue number if applicable.

## Outside Contributors

* Discuss your intended changes with the core team on Github
* Announce that you are working or want to work on a specific issue
* Avoid large pull requests - they are much less likely to be merged as they are incredibly hard to review

## Pull Requests

* Do not commit/push directly to the main branch. Instead, create a fork and file a pull request.
* When maintaining a branch, merge frequently with the main.
* When maintaining a branch, submit pull requests to the main frequently.
* If you are working on a bigger issue try to split it up into several smaller issues.
* Please do not open "Draft" pull requests. Rather, use issues or discussion topics to discuss whatever needs discussing.
* We reserve full and final discretion over whether or not we will merge a pull request. Adhering to these guidelines is not a complete guarantee that your pull request will be merged.

## CI for pull requests

* Pull requests will need to be pass all continuous integration checks before merging.
* For faster iteration and more control, consider running CI on your own fork or when possible directly locally.
* Submitting changes to a open pull requests will move it to 'draft' state.
* Pull requests will get a complete run on the main repo CI only when marked as 'ready for review' (via Web UI, button on bottom right).

## Nightly CI

* Packages creation and long running tests will be performed during a nightly run
* On your fork you can trigger long running tests (NightlyTests.yml) for any branch following information from https://docs.github.com/en/actions/using-workflows/manually-running-a-workflow#running-a-workflow

## Building

* To build the project, run `make`.
* To build the project for debugging, run `make debug`.
* To build optional components, use the flags defined in the Makefile, e.g. to build the JDBC driver, run `BUILD_JDBC=1 make`.
* For parallel builds, you can use the [Ninja](https://ninja-build.org/) build system: `GEN=ninja make`.

## Testing

* Unit tests can be written either using the sqllogictest framework (`.test` files) or in C++ directly. We **strongly** prefer tests to be written using the sqllogictest framework. Only write tests in C++ if you absolutely need to (e.g. when testing concurrent connections or other exotic behavior).
* Documentation for the testing framework can be found [here](https://duckdb.org/dev/testing).
* Write many tests.
* Test with different types, especially numerics, strings and complex nested types.
* Try to test unexpected/incorrect usage as well, instead of only the happy path.
* `make unit` runs the **fast** unit tests (~one minute), `make allunit` runs **all** unit tests (~one hour).
* Make sure **all** unit tests pass before sending a PR.
* Slower tests should be added to the **all** unit tests. You can do this by naming the test file `.test_slow` in the sqllogictests, or by adding `[.]` after the test group in the C++ tests.
* Look at the code coverage report of your branch and attempt to cover all code paths in the fast unit tests. Attempt to trigger exceptions as well. It is acceptable to have some exceptions not triggered (e.g. out of memory exceptions or type switch exceptions), but large branches of code should always be either covered or removed.
* DuckDB uses GitHub Actions as its continuous integration (CI) tool. You have the option to run GitHub Actions on your forked repository. For detailed instructions, you can refer to the [GitHub documentation](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/enabling-features-for-your-repository/managing-github-actions-settings-for-a-repository). Before running GitHub Actions, please ensure that you have all the Git tags from the duckdb/duckdb repository. To accomplish this, execute the following commands `git fetch <your-duckdb/duckdb-repo-remote-name> --tags` then 
`git push --tags` These commands will fetch all the git tags from the duckdb/duckdb repository and push them to your forked repository. This ensures that you have all the necessary tags available for your GitHub Actions workflow. 

## Formatting

* Use tabs for indentation, spaces for alignment.
* Lines should not exceed 120 columns.
* To make sure the formatting is consistent, please use version 10.0.1, installable through `python3 -m pip install clang-format==10.0.1.1`
* `clang_format` and `black` enforce these rules automatically, use `make format-fix` to run the formatter.
* The project also comes with an [`.editorconfig` file](https://editorconfig.org/) that corresponds to these rules.

## C++ Guidelines

* Do not use `malloc`, prefer the use of smart pointers. Keywords `new` and `delete` are a code smell.
* Strongly prefer the use of `unique_ptr` over `shared_ptr`, only use `shared_ptr` if you **absolutely** have to.
* Use `const` whenever possible.
* Do **not** import namespaces (e.g. `using std`).
* All functions in source files in the core (`src` directory) should be part of the `duckdb` namespace.
* When overriding a virtual method, avoid repeating virtual and always use `override` or `final`.
* Use `[u]int(8|16|32|64)_t` instead of `int`, `long`, `uint` etc. Use `idx_t` instead of `size_t` for offsets/indices/counts of any kind.
* Prefer using references over pointers as arguments.
* Use `const` references for arguments of non-trivial objects (e.g. `std::vector`, ...).
* Use C++11 for loops when possible: `for (const auto& item : items) {...}`
* Use braces for indenting `if` statements and loops. Avoid single-line if statements and loops, especially nested ones.
* **Class Layout:** Start out with a `public` block containing the constructor and public variables, followed by a `public` block containing public methods of the class. After that follow any private functions and private variables. For example:
    ```cpp
    class MyClass {
    public:
    	MyClass();

    	int my_public_variable;

    public:
    	void MyFunction();

    private:
    	void MyPrivateFunction();

    private:
    	void my_private_variable;
    };
    ```
* Avoid [unnamed magic numbers](https://en.wikipedia.org/wiki/Magic_number_(programming)). Instead, use named variables that are stored in a `constexpr`.
* [Return early](https://medium.com/swlh/return-early-pattern-3d18a41bba8). Avoid deep nested branches.
* Do not include commented out code blocks in pull requests.

## Error Handling

* Use exceptions **only** when an error is encountered that terminates a query (e.g. parser error, table not found). Exceptions should only be used for **exceptional** situations. For regular errors that do not break the execution flow (e.g. errors you **expect** might occur) use a return value instead.
* Try to add test cases that trigger exceptions. If an exception cannot be easily triggered using a test case then it should probably be an assertion. This is not always true (e.g. out of memory errors are exceptions, but are very hard to trigger).
* Use `D_ASSERT` to assert. Use **assert** only when failing the assert means a programmer error. Assert should never be triggered by user input. Avoid code like `D_ASSERT(a > b + 3);` without comments or context.
* Assert liberally, but make it clear with comments next to the assert what went wrong when the assert is triggered.

## Naming Conventions

* Choose descriptive names. Avoid single-letter variable names.
* Files: lowercase separated by underscores, e.g., abstract_operator.cpp
* Types (classes, structs, enums, typedefs, using): CamelCase starting with uppercase letter, e.g., BaseColumn
* Variables: lowercase separated by underscores, e.g., chunk_size
* Functions: CamelCase starting with uppercase letter, e.g., GetChunk
* Avoid `i`, `j`, etc. in **nested** loops. Prefer to use e.g. **column_idx**, **check_idx**. In a **non-nested** loop it is permissible to use **i** as iterator index.
* These rules are partially enforced by `clang-tidy`.
