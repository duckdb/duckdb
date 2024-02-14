## Adding a new Connect Attribute

### 1. Add the attribute to the JSON file
In the [connection_string_attributes.json](./connection_string_attributes.json) file, add a new attribute to the `attributes` array. The following fields are required:
- `keyword`: the keyword associated with the attribute
- `enum`: an enum value that represents the attribute in CAPS
- `description`: a description of the attribute
- `options`: if the attribute has a set of options, list them here in a string. If not, set to an empty string.
- `example`: an example of the attribute in a connection string
- `default`: the default value of the attribute

### Example
```json
{
      "keyword": "access_mode",
      "enum": "ACCESS_MODE",
      "description": "The access mode of the database.",
      "options": "READ_ONLY, READ_WRITE",
      "example": "READ_WRITE",
      "default": "READ_WRITE"
}
```

### 2. Run the Python script
Run the following python script from the project directory to regenerate the [`connect.hpp`](./include/connect.hpp) file:

```bash
python3 tools/odbc/scripts/generate_connection_string_attributes.py
```

Do not change the `connect.hpp` file directly, as it will be overwritten by the script.

### 3. Implement the Attribute function in [`connect.cpp`](./connect.cpp)
A function signature for the attribute has been generated in the `connect.hpp` file,
which needs to be implemented in the `connect.cpp` file.
The functions return a `SQLRETURN` and take a `const string &` as an argument.

For example, the `access_mode` attribute would look like this:

```cpp
SQLRETURN Connect::HandleAllowUnsignedExtensions(const string &val) {
	if (duckdb::StringUtil::Lower(val) == "true") {
		config.options.allow_unsigned_extensions = true;
	}
	set_keys[UNSIGNED] = true;
	return SQL_SUCCESS;
}
```

### 4. Add a test for the attribute
The final step is to add a test for the attribute in the [test/tests directory](./test/tests).
Feel free to either add a new test file or add the test to the [`connect` tester file](./test/tests/connect.cpp).

## Running the ODBC Client and Tests

#### Build the ODBC client (from within the main DuckDB repository)

```bash
BUILD_ODBC=1 DISABLE_SANITIZER=1 make debug -j
```

#### Run the ODBC Unit Tests

The ODBC tests are written with the catch framework. To run the tests, run the following command from the main DuckDB repository:

```bash
build/debug/tools/odbc/test/test_odbc
```

You can also individually run the tests by specifying the test name as an argument to the test executable:

```bash
build/debug/tools/odbc/test/test_odbc 'Test ALTER TABLE statement'
```