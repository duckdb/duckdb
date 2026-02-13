# DuckDB PEG Parser Grammar

This document describes the PEG (Parsing Expression Grammar) system used by DuckDB's autocomplete extension, including the grammar syntax, build process, and transformer code generation.

## Grammar Syntax

Grammar rules are defined in `.gram` files located in `extension/autocomplete/grammar/statements/`. Each file typically corresponds to one SQL statement type (e.g., `select.gram`, `insert.gram`, `alter.gram`).

### Rule Definitions

Rules are defined with the `<-` operator:

```
RuleName <- RuleDefinition
```

### Literals

Keywords and operators are matched with single-quoted strings:

```
'CREATE'  'TABLE'  'SELECT'  ','  '('  ')'
```

Matching is case-insensitive by default for keyword literals.

### Choices (Ordered Alternatives)

The `/` operator tries alternatives left-to-right. The first successful match wins (PEG semantics — no ambiguity):

```
DistinctOrAll <- 'DISTINCT' / 'ALL'
LiteralExpression <- StringLiteral / NumberLiteral / ConstantLiteral
```

**Important**: Because PEG uses ordered choice, more specific alternatives must come before general ones. For example, compound interval types like `YEAR TO MONTH` must precede the simple `YEAR` keyword, otherwise the parser greedily consumes `YEAR` and fails on `TO MONTH`.

### Sequences

Elements listed one after another form a sequence — all must match in order:

```
CastExpression <- CastOrTryCast Parens(Expression 'AS' Type)
UpdateSetClause <- 'SET' List(UpdateSetExpression)
```

### Optional (`?`)

The `?` operator makes an element optional (zero or one):

```
CreateStatement <- 'CREATE' OrReplace? Temporary? CreateStatementVariation
SelectClause <- 'SELECT' DistinctClause? TargetList
```

### Repeat (`*` and `+`)

- `*` — zero or more occurrences
- `+` — one or more occurrences

```
LogicalNotExpression <- 'NOT'* IsExpression
CaseExpression <- 'CASE' Expression? CaseWhenThen+ CaseElse? 'END'
```

### Grouping with Parentheses

Parentheses group sub-expressions without creating a named rule:

```
Interval <- (YearKeyword 'TO' MonthKeyword) /
    (DayKeyword 'TO' HourKeyword) /
    YearKeyword /
    MonthKeyword
```

### Parameterized Rules (Macros)

Rules can accept parameters, acting as reusable templates. Two built-in macros are defined in `common.gram`:

```
List(D) <- D (',' D)* ','?
Parens(D) <- '(' D ')'
```

Usage:

```
Parens(List(Expression))                        -- (expr, expr, ...)
Parens(DistinctOrAll? List(FunctionArgument)?)   -- Nesting allowed
List(BaseTableName)                              -- table1, table2, ...
```

### Negative Lookahead (`!`)

The `!` operator asserts that a pattern does NOT match at the current position (without consuming input):

```
PlainIdentifier <- !ReservedKeyword <[a-z_]i[a-z0-9_]i*>
```

**Note**: Negative lookahead is parsed but currently ignored by the matcher.
TODO(Dtenwolde): Implement negative lookahead

### Special Tokens

Some rules are handled directly by the matcher rather than by grammar expansion. These include:

- `Identifier` / `ReservedIdentifier` — variable/identifier matching
- `TableName`, `ColumnName`, `SchemaName`, etc. — context-aware identifier matching
- `StringLiteral` — single-quoted string literals
- `NumberLiteral` — numeric literals
- `OperatorLiteral` — operator symbols

See `matcher.cpp` for a full list. 

### Comments

Lines starting with `#` are comments:

```
# Expression precedence levels
# LEVEL 1 (Lowest)
Expression <- LambdaArrowExpression
```

### Keywords

Keywords are defined in list files under `grammar/keywords/`:

| File | Description |
|------|-------------|
| `reserved_keyword.list` | Cannot be used as identifiers (e.g., `SELECT`, `FROM`) |
| `unreserved_keyword.list` | Can be used as identifiers (e.g., `ABORT`, `ABSOLUTE`) |
| `column_name_keyword.list` | Allowed as column names |
| `func_name_keyword.list` | Allowed as function names |
| `type_name_keyword.list` | Allowed as type names |

A keyword must appear in exactly one list. The build script validates this.

## Building the Grammar

After modifying `.gram` files or keyword lists, regenerate the inlined grammar files:

```bash
./scripts/build_peg_grammar.sh
```

**Prerequisites**: A Python virtual environment at `.venv` in the repository root.

This script runs `extension/autocomplete/inline_grammar.py` twice:

1. **`--grammar-file`**: Combines all `.gram` files and keyword lists into a single `inlined_grammar.gram` (useful for debugging).
2. **Default**: Generates `inlined_grammar.hpp` (C++ header embedding the grammar as a string constant) and `keyword_map.cpp` (keyword lookup tables).

### Generated Files

| File | Description |
|------|-------------|
| `include/inlined_grammar.gram` | Complete merged grammar (all `.gram` files + keywords) |
| `include/inlined_grammar.hpp` | C++ header: `const char INLINED_PEG_GRAMMAR[]` |
| `keyword_map.cpp` | C++ keyword category lookup maps |

**Do not edit generated files manually** — they will be overwritten by the build script.

## Implementing Transformer Rules

Each grammar rule that produces a parse result needs a corresponding transformer function to convert it into a DuckDB AST node (`SQLStatement`, `QueryNode`, `ParsedExpression`, etc.).

### Checking Coverage

Run the coverage checker to see which rules need transformers:

```bash
python scripts/generate_peg_transformer.py
```

Output labels:

| Label | Meaning |
|-------|---------|
| `[ FOUND ]` | Transformer implemented and registered |
| `[ ENUM ]` | Handled as an enum mapping |
| `[ NOT REG'D ]` | Transformer exists but not registered in factory |
| `[ MISSING ]` | No transformer implementation |
| `[ EXCLUDED ]` | Intentionally excluded (handled by matcher directly) |

Use `-s` to skip `[ FOUND ]` and `[ ENUM ]` rules for a cleaner view:

```bash
python scripts/generate_peg_transformer.py -s
```

### Generating Stubs

Use the `-g` flag to generate skeleton code for missing rules:

```bash
python scripts/generate_peg_transformer.py -g
```

For each missing rule, this generates three pieces of code:

**1. Declaration** (add to `include/transformer/peg_transformer.hpp`):

```cpp
static unique_ptr<SQLStatement> TransformRuleName(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result);
```

**2. Registration** (add to `transformer/peg_transformer_factory.cpp` inside the appropriate `Register...()` function):

```cpp
REGISTER_TRANSFORM(TransformRuleName);
```

**3. Implementation** (add to the corresponding `transformer/transform_*.cpp` file):

```cpp
unique_ptr<SQLStatement> PEGTransformerFactory::TransformRuleName(PEGTransformer &transformer,
                                                                   optional_ptr<ParseResult> parse_result) {
    throw NotImplementedException("TransformRuleName has not yet been implemented");
}
```

**Note**: The generated return type (`unique_ptr<SQLStatement>`) is a placeholder. You must change it to match what the rule actually produces (e.g., `unique_ptr<QueryNode>`, `unique_ptr<SelectStatement>`, `QualifiedName`, `ShowType`, etc.).

### Transformer Implementation Pattern

A typical transformer function:

1. Casts the parse result to the expected type (usually `ListParseResult` for sequences, `ChoiceParseResult` for alternatives)
2. Extracts children by index (matching the grammar rule's element order)
3. Recursively transforms children via `transformer.Transform<T>(child)`
4. Constructs and returns the appropriate AST node

Example for `ShowTables <- ShowOrDescribe 'TABLES' 'FROM' QualifiedName`:

```cpp
unique_ptr<QueryNode> PEGTransformerFactory::TransformShowTables(PEGTransformer &transformer,
                                                                  optional_ptr<ParseResult> parse_result) {
    auto &list_pr = parse_result->Cast<ListParseResult>();
    // Child 0: ShowOrDescribe, Child 1: 'TABLES', Child 2: 'FROM', Child 3: QualifiedName
    auto qualified_name = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(3));
    auto showref = make_uniq<ShowRef>();
    showref->show_type = ShowType::SHOW_FROM;
    showref->schema_name = qualified_name.name;
    // ... build and return the AST node
}
```

### Parse Result Types

| Grammar Construct | Parse Result Type | Access Pattern |
|-------------------|-------------------|----------------|
| Sequence (`A B C`) | `ListParseResult` | `list_pr.Child<T>(index)`, `list_pr.GetChild(index)` |
| Choice (`A / B`) | `ChoiceParseResult` | `choice_pr.result` (the matched alternative) |
| Optional (`A?`) | `OptionalParseResult` | `opt_pr.HasResult()`, `opt_pr.optional_result` |
| Repeat (`A+` / `A*`) | `RepeatParseResult` | Iterable list of results |
| Identifier | `IdentifierParseResult` | `.result` (string) |
| String literal | `StringLiteralParseResult` | `.result` (string), `.ToExpression()` (ParsedExpression) |
| Number literal | `NumberParseResult` | `.result` (string) |
| Keyword | `KeywordParseResult` | `.result` (string) |

### Registration

Transforms are registered in category-specific methods in `peg_transformer_factory.cpp`:

```cpp
void PEGTransformerFactory::RegisterDescribe() {
    REGISTER_TRANSFORM(TransformDescribeStatement);
    REGISTER_TRANSFORM(TransformShowTables);
    REGISTER_TRANSFORM(TransformShowAllTables);
    // ...
}
```

The `REGISTER_TRANSFORM` macro strips the `Transform` prefix to derive the grammar rule name, so `TransformShowTables` registers for the `ShowTables` grammar rule.
