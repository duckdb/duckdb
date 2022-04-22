
#### Installing the Package


#### Formatting
The format script must be run when changing anything. This can be done by running the following command from within the root directory of the project:

```bash
julia tools/juliapkg/scripts/format.jl
```

#### Testing

You can run the tests using the `test.sh` script:

```
./test.sh
```

Specific test files can be run by adding the name of the file as an argument:

```
./test.sh test_connection.jl
```

#### Original Julia Connector
Credits to kimmolinna for the [original DuckDB Julia connector](https://github.com/kimmolinna/DuckDB.jl).