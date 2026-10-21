# DuckDB PEG Parser Grammar

This document describes the PEG (Parsing Expression Grammar) system that DuckDB uses to parse SQL, including the grammar syntax, build process, and transformer code generation.

## Grammar Syntax

Grammar rules are defined in `.gram` files located in `src/parser/peg/grammar/statements/`. Each file typically corresponds to one SQL statement type (e.g., `select.gram`, `insert.gram`, `alter.gram`). Shared rules and macros live in `common.gram`.

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
DistinctOrAll <- DistinctKeyword / AllKeyword
LiteralExpression <- StringLiteral / NumberLiteral / ConstantLiteral
```

**Important**: Because PEG uses ordered choice, more specific alternatives must come before general ones. For example, compound interval types like `YEAR TO MONTH` must precede the simple `YEAR` keyword, otherwise the parser greedily consumes `YEAR` and fails on `TO MONTH`.

### Sequences

Elements listed one after another form a sequence — all must match in order:

```
CastExpression <- CastOrTryCast Parens(CastArguments)
ShowTables <- ShowOrDescribe 'TABLES' 'FROM' QualifiedName
```

### Optional (`?`)

The `?` operator makes an element optional (zero or one):

```
CreateStatement <- 'CREATE' OrReplace? Temporary? CreateStatementVariation
SelectClause <- 'SELECT' DistinctClause? TargetList?
```

### Repeat (`*` and `+`)

- `*` — zero or more occurrences
- `+` — one or more occurrences

```
IsExpression <- IsDistinctFromExpression IsTest*
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

**Note**: Negative lookahead is parsed but currently ignored by the matcher (see the `FIXME` in `peg_parser.cpp`).
Restrict which keywords a rule accepts through the keyword lists and identifier matchers instead.

### Special Tokens

Some rules are handled by dedicated matchers rather than by grammar expansion. These are registered as terminal rule
overrides in `compiled_grammar.cpp` (between the `GENERATED RULE OVERRIDES` markers) and configured in the
`matcher_rule_overrides` section of `scripts/parser/grammar_types.yml`. They include:

- `Identifier` / `ReservedIdentifier` — generic identifier matching
- `CatalogName`, `SchemaName`, `TableName`, `ColumnName`, `IndexName`, `SequenceName`, `TableFunctionName`,
  `TypeName`, `PragmaName`, `SettingName`, `CopyOptionName` and their `Reserved*` variants — context-aware identifiers
- `ReservedKeyword` — any reserved keyword
- `StringLiteral` — single-quoted string literals
- `NumberLiteral` — numeric literals
- `OperatorLiteral` — operator symbols
- `EndOfInput` — end of the statement

### Comments

Lines starting with `#` are comments:

```
# Expression precedence levels
# LEVEL 1 (Lowest)
Expression <- LambdaArrowExpression
```

### Keywords

Keywords are defined in list files under `src/parser/peg/grammar/keywords/`:

| File | Description |
|------|-------------|
| `reserved_keyword.list` | Cannot be used as identifiers (e.g., `SELECT`, `FROM`) |
| `unreserved_keyword.list` | Can be used as identifiers (e.g., `ABORT`, `ABSOLUTE`) |
| `column_name_keyword.list` | Allowed as column names |
| `func_name_keyword.list` | Allowed as function names |
| `type_name_keyword.list` | Allowed as type names |

A keyword must not appear in both the reserved and unreserved lists; `inline_grammar.py` rejects such conflicts.

## Building the Grammar

After modifying `.gram` files, keyword lists, or any of the generator configuration files, regenerate the grammar and
transformer artifacts:

```bash
make parser-grammar
```

This target installs the Python dependencies (PyYAML) into the formatting virtualenv and runs
`scripts/parser/build_grammar.sh`, which performs these steps in order:

1. `scripts/parser/inline_grammar.py --grammar-file` — combines all `.gram` files and keyword lists into
   `inlined_grammar.gram` (useful for debugging).
2. `scripts/parser/inline_grammar.py` — generates `inlined_grammar.hpp` (the grammar embedded as a byte array) and
   `keyword_map.cpp` (keyword lookup tables).
3. `scripts/parser/generate_transformer.py --write` — generates the typed transformer wrappers (see below).
4. `scripts/parser/generate_transformer_trampoline.py --write` — generates the trampoline transform processes (see below).
5. `make format-parser-grammar` — formats the generated C++ files.

The generators can also be run directly (with `--write`) when only the transformer artifacts need regenerating. Without
`--write` they print what would be generated instead of modifying files.

### Generated Files

| File | Generated by | Description |
|------|--------------|-------------|
| `src/include/duckdb/parser/peg/inlined_grammar.gram` | `inline_grammar.py` | Complete merged grammar (all `.gram` files + keywords) |
| `src/include/duckdb/parser/peg/inlined_grammar.hpp` | `inline_grammar.py` | C++ header: `const unsigned char INLINED_PEG_GRAMMAR[]` |
| `src/parser/peg/keyword_map.cpp` | `inline_grammar.py` | C++ keyword category lookup maps |
| `src/parser/peg/transformer/transform_generated.cpp` | `generate_transformer.py` | `*Internal` wrappers and the `RegisterGenerated()` table |
| `src/parser/peg/transformer/transform_generated_trampoline.cpp` | `generate_transformer_trampoline.py` | Trampoline `Initialize*`/`Finalize*` processes and frame-ops table |
| `src/include/duckdb/parser/peg/transformer/peg_transformer.hpp` | both transformer generators | Declarations between the `GENERATED RULES` and `GENERATED TRAMPOLINE RULES` markers |
| `src/parser/peg/compiled_grammar.cpp` | `generate_transformer.py` | Terminal rule overrides between the `GENERATED RULE OVERRIDES` markers |
| `src/parser/peg/matcher_factory.cpp` | `generate_transformer.py` | Packrat-memoized rule list between the `GENERATED PACKRAT MEMOIZED RULES` markers |

**Do not edit generated files or generated blocks manually** — they will be overwritten by the build script. Files that
are only partially generated contain `START GENERATED ... ` / `END GENERATED ...` marker comments around the
generated block; code outside the markers is hand-written.

## Implementing Transformer Rules

Each grammar rule that produces a value needs a transformer that converts its parse result into a DuckDB AST node
(`SQLStatement`, `QueryNode`, `ParsedExpression`, etc.) or an intermediate helper type.

Most of the parse-result plumbing is generated. For a grammar rule `Foo`, there are up to three functions:

| Function | Written by | Purpose |
|----------|------------|---------|
| `TransformFooInternal(transformer, parse_result)` | generated | Unpacks the parse result, transforms the children, calls the typed body, boxes the result |
| `InitializeFooTrampoline` / `FinalizeFooTrampoline` | generated | Same as `*Internal`, but as a two-phase process on the explicit transform stack (used to avoid deep recursion) |
| `TransformFoo(transformer, typed children...)` | hand-written | The semantic body: builds the AST node from already-transformed children |

Rules that consist only of literals, or choice rules whose alternatives all return the same type, are generated end-to-end and need no hand-written body.

### Generator Configuration

`scripts/parser/grammar_types.yml` drives `generate_transformer.py`:

- Type categories map grammar rules to their C++ return type (`unique_ptr<SQLStatement>`, `QualifiedName`, ...).
  Every rule that is referenced as a child of a generated rule must have a type here.
- `overrides` lists rules with a unique return type.
- `excluded_rules` lists syntax-only rules that are skipped when generating wrappers.
- `matcher_rule_overrides` lists the special tokens handled by dedicated matchers (see above).
- `packrat_memoized_rules` lists rules that use packrat memoization in the matcher.

`scripts/parser/transformer_trampoline.yml` drives `generate_transformer_trampoline.py`. Rules not listed there get a
generated trampoline. Listed rules are either `excluded`, `forward` (delegate to a single child), or `manual` /
`manual_finalize` with hand-written `init` / `finalize` functions.

### Adding or Changing a Rule

1. Add or edit the rule in the relevant `.gram` file.
2. Add the rule's return type to `grammar_types.yml` (or `excluded_rules` if it is syntax only).
3. Run `make parser-grammar`. The generator prints the remaining manual steps, including the exact signature of any
   typed body that still needs to be hand-written.
4. Implement the body in the matching `transform_*.cpp` file (e.g. rules from `describe.gram` go in
   `transform_describe.cpp`), then run `make parser-grammar` again so the declarations pick it up.

### Transformer Implementation Pattern

The hand-written body receives already-transformed children in grammar order; literal tokens are skipped. For
`ShowTables <- ShowOrDescribe 'TABLES' 'FROM' QualifiedName`:

```cpp
unique_ptr<QueryNode> PEGTransformerFactory::TransformShowTables(PEGTransformer &transformer,
                                                                 const ShowType &show_or_describe,
                                                                 const QualifiedName &qualified_name) {
	auto showref = make_uniq<ShowRef>();
	showref->show_type = ShowType::SHOW_FROM;
	showref->SetSchemaName(qualified_name.Name());
	// ... build and return the AST node
}
```

The generated wrapper that calls it looks like this (in `transform_generated.cpp`):

```cpp
unique_ptr<TransformResultValue> PEGTransformerFactory::TransformShowTablesInternal(PEGTransformer &transformer,
                                                                                    ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto show_or_describe = transformer.Transform<ShowType>(list_pr.GetChild(0));
	auto qualified_name = transformer.Transform<QualifiedName>(list_pr.GetChild(3));
	auto result = TransformShowTables(transformer, show_or_describe, qualified_name);
	return make_uniq<TypedTransformResult<unique_ptr<QueryNode>>>(std::move(result));
}
```

Optional children arrive as `optional<T>` (or `unique_ptr<T>` that may be null) and repeated children as `vector<T>`.
Only the few rules that the generator cannot handle (listed under `excluded_rules`, or `manual` in the trampoline
config) still unpack the parse result by hand, using the types below.

### Parse Result Types

| Grammar Construct | Parse Result Type | Access Pattern |
|-------------------|-------------------|----------------|
| Sequence (`A B C`) | `ListParseResult` | `list_pr.Child<T>(index)`, `list_pr.GetChild(index)`, `GetChildren()` |
| Choice (`A / B`) | `ChoiceParseResult` | `choice_pr.GetResult()` (the matched alternative) |
| Optional (`A?`) | `OptionalParseResult` | `opt_pr.HasResult()`, `opt_pr.GetResult()` |
| Repeat (`A+` / `A*`) | `RepeatParseResult` | `repeat_pr.GetChildren()` |
| Identifier | `IdentifierParseResult` | `.identifier` (`Identifier`) |
| String literal | `StringLiteralParseResult` | `.result` (string), `.ToExpression()` (ParsedExpression) |
| Number literal | `NumberParseResult` | `.number` (string) |
| Keyword | `KeywordParseResult` | `.keyword` (string) |
| Operator | `OperatorParseResult` | `.operator_token` (string) |
| End of input | `EndOfInputParseResult` | — |

`PEGTransformerFactory::ExtractParseResultsFromList` and `ExtractResultFromParens` unwrap the `List(D)` and
`Parens(D)` macros when working with raw parse results.

### Registration

Generated transforms are registered automatically: `RegisterGenerated()` in `transform_generated.cpp` registers every
`*Internal` wrapper by rule name, and the frame-ops table in `transform_generated_trampoline.cpp` installs the
trampoline processes. Both are invoked from the `PEGTransformerFactory` constructor in `peg_transformer_factory.cpp`.

The handful of fully hand-written transforms (literals, identifiers, keywords, and a few expression and select rules)
are registered in category-specific methods in the same file:

```cpp
void PEGTransformerFactory::RegisterCommon() {
	REGISTER_TRANSFORM(TransformNumberLiteral);
	REGISTER_TRANSFORM(TransformStringLiteral);
	// ...
}
```

The `REGISTER_TRANSFORM` macro strips the `Transform` prefix to derive the grammar rule name, so `TransformNumberLiteral` registers for the `NumberLiteral` grammar rule.

## Extending the Grammar from Extensions

Extensions can modify the grammar at runtime without touching the `.gram` files. The relevant headers live in
`src/include/duckdb/parser/`.

### GrammarExtension and GrammarChange

A `GrammarExtension` (`grammar_extension.hpp`) is a named, composable set of `GrammarChange`s
(`grammar_change.hpp`) that is applied on top of the built-in grammar when the parser is compiled
(`CompiledGrammar::Create` in `compiled_grammar.cpp`). Available changes:

| Change | Effect |
|--------|--------|
| `AddRule(definition, transform_process)` | Add a new rule, optionally with a transform process for it |
| `ReplaceRule(definition, transform_process)` | Replace an existing rule |
| `AddChoice(rule, choice)` / `PrependChoice(rule, choice)` | Append or prepend an alternative to a choice rule |
| `RemoveChoice(rule, find_cursor)` / `ReplaceChoice(rule, choice, find_cursor)` | Remove or replace an alternative |
| `SetTransformProcess(rule, transform_process)` | Attach a transform process to an existing rule |
| `AddTerminalRuleOverride(rule, matcher_factory)` | Replace a rule with a custom `Matcher` |

Choices can reference built-in rules, so `GrammarChange::AddChoice("UnreservedKeyword", "'ANSWER'")` adds a keyword and
`GrammarChange::PrependChoice("SelectAtom", "PipeSelectAtom")` hooks a new statement form into `SELECT`.

Register the extension with `GrammarExtension::Register(db, make_shared_ptr<MyGrammarExtension>())`, usually from the
extension's load function. Registered extensions are listed by `duckdb_grammar_extensions()` and are inactive by
default; a connection enables them with:

```sql
SET active_grammar_extensions = ['pipe_query_syntax'];
```

Transforms for added rules are written against the raw parse-result types described above, since the generators only
run over the in-tree grammar. See `test/extension/loadable_grammar_extension_demo.cpp` for a complete example that adds
pipe syntax (`FROM tbl |> WHERE ... |> SELECT ...`), and `test/api/test_grammar_extension.cpp` for a minimal one that
also adds a custom matcher.

### DialectExtension

A `DialectExtension` (`dialect_extension.hpp`) registers a named SQL dialect with
`DialectExtension::Register(config, DialectExtension("name"))`. Registered dialects are listed by `duckdb_dialects()`
and can be selected with `SET current_dialect = 'name'`; the built-in dialect is `duckdb`. The dialect name is
currently only validated and exposed through these settings, so a dialect still has to register its grammar changes
through a `GrammarExtension`.
