The grammar subdirectory contains the SQL parser of DuckDB. It is heavily based on the PostgreSQL parser, however, the parser itself is generated from a set of different files in this directory rather than being created and maintained as a single huge yacc file.

The grammar folder contains three subdirectories:

### keywords
This subdirectory contains lists of all the keywords that have special meanings within the SQL language.

### statements
This subdirectory contains the code used to parse different statement types. Statements are what is parsed at the top level of

The main.y file provides the main skeleton for the yacc file. This will be used to construct the final yacc file through a series of string replacements.

* `grammar.hpp`: This file contains the C++ code that is included in the header of the generated grammar. Generally this contains function prototypes that are used during parsing.
* `grammar.cpp`: This file contains the C++ code that is included in the source file of the generated grammar. Generally this contains implementations of functions used during parsing.