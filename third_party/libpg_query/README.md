# Modified Postgres Parser

This directory holds the core parser that is used by DuckDB. It is based on the [Postgres parser](https://github.com/pganalyze/libpg_query), but has been stripped down and generally cleaned up to make it easier to modify and extend.

The most important changes made are listed here:
* The output format has been changed to C++
* The parser and all its auxiliary structures are wrapped in the `duckdb_libpgquery` namespace.
* The parser has been split up into multiple different files instead of living in one big file.
* Duplication is reduced and code is simplified by e.g. not requiring the same keyword or statement to be declared in multiple different places. 

# Compiling the Parser
The parser is converted to C++ using flex/bison. It is usually run on a Macbook, and has been tested with the following versions of flex/bison:

```bash
yac --flex 2.5.35 Apple(flex-32)
bison (GNU Bison) 2.3
```

In order to compile the grammar, run the following command:

```bash
python3 scripts/generate_grammar.py
```
Depending on how you installed bison, you may need to pass `--bison=/usr/bin/bison` to the above command.

In order to compile the lexer, run the following command:

```bash
python3 scripts/generate_flex.py
```

# Modifying the Grammar
In order to modify the grammar, you will generally need to change files in the `third_party/libpg_query/grammar` directory.

The grammar is divided in separate files in the `statements` directory. The bulk of the grammar (expressions, types, etc) are found in the `select.y` file. This is likely the file you will want to edit.

### Short Bison Intro
Bison is defined in terms of rules. Each rule defines a *return type*. The return types of the rules should be defined in the corresponding `.yh` file (for example, for the `select.y`, the return types are defined in `third_party/libpg_query/grammar/types/select.yh`).

Rules generally reference other rules using the grammar. Each rule contains inline C code. The C code describes how to compose the result objects from the grammar.

The result objects are primarily defined in `third_party/libpg_query/include/nodes/parsenodes.hpp`, and are mostly taken over from Postgres. The parse nodes are converted into DuckDB's internal representation in the Transformer phase. 

As an example, let's look at a rule:

### Rule Example
```yacc
from_list:
            table_ref                   { $$ = list_make1($1); }
            | from_list ',' table_ref   { $$ = lappend($1, $3); }
        ;
```

We see here the rule with the name `from_list`. This describes the list of tables found in the FROM clause of a SELECT statement. 

Rules can be composed using a chain of one or more OR statements (`|`). The rule can be satisfied using either section of the OR statement.

As we see, the rules can also be recursive. In this case, we can have as many table references as we like. For example, the following are all valid SQL:

```sql
SELECT * FROM a;
SELECT * FROM a, b;
SELECT * FROM a, b, c, d, e, f;
```

Recursive statements are generally used to generate lists. The recursion happens on the left side, and a new list is created in the left-most element.

Note that in the generated C code we see two special symbols: `$$` and `$1`. `$$` is the *return value* of the rule. The `$1, $2, $3, etc` symbols refer to the sub-rules.

For example, in this part of the statement:
```yacc
   from_list ',' table_ref   { $$ = lappend($1, $3); }
```
We are really saying: take the return value of `from_list` (a list) and the return value of `table_ref` (a node) and append it to the list. That list then becomes the return value. Note that we use `$3` to refer to `table_ref`. That is because the second rule (`$2`)  is actually the comma in the middle.

`list_make1` and `lappend` are regular C functions defined by Postgres (see `third_party/libpg_query/include/nodes/pg_list.hpp`).

### Return Values
As stated before, rules have return values. The return values must be defined in the corresponding `.yh` files. For example, for `select.y`, the corresponding `.yh` file is `third_party/libpg_query/grammar/types/select.yh`. 

We can find the following type definitions there for our previous example:

```yacc
%type <list> from_list
%type <node> table_ref
```

The names given here correspond to C objects and are defined in `third_party/libpg_query/grammar/grammar.y`.
```c
%union
{
  ...
  PGList *list;
  PGNode *node;
  ...
};
```

Most rules return either a `PGNode` (which is the parent class of almost all parse nodes), or a `PGList` (generally a list of nodes). Other common return values are `ival`, `str`, `boolean` and `value`.

### Adding a Keyword
Keywords are defined in the various keyword lists in `third_party/libpg_query/grammar/keywords`.

```
unreserved_keywords.list
column_name_keywords.list
func_name_keywords.list
type_name_keywords.list
reserved_keywords.list
```

The list in which you add a keyword determines where the keyword can be used.

It is heavily preferred that keywords are added to the `unreserved_keywords.list`. Keywords added to this list can still be used as column names, type names and function names by users of the system.

The `reserved_keywords.list` on the other hand heavily restricts the usage of the specific keyword. It should only be used if there is no other option, as reserved keywords can break people's code.

The other lists exist for keywords that are not *entirely* reserved, but are partially reserved. 
* A keyword in `column_name_keywords.list` can be used as a column name.
* A keyword in `func_name_keywords.list` can be used as a function name.
* A keyword in `type_name_keywords.list` can be used as a type name.

Note that these lists are not mutually exclusive. A keyword can be added to *both* `column_name_keywords.list` and `func_name_keywords.list`.

Adding a keyword to all three of these lists is equivalent to adding an unreserved keyword (and hence the keyword should just be added there).

### Adding a Statement Type
A new statement type can be defined by creating two new files (`new_statement.y` and `new_statement.yh`), and adding the top-level statement to the list of statements (`third_party/libpg_query/grammar/statements.list`).

As an example, it is recommended to look at the PragmaStmt (`pragma.y`).

# Modifying the Lexer
The lexer file is provided in `third_party/libpg_query/scan.l`. After changing the lexer, run `python3 scripts/generate_flex.py`. 
