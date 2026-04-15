/*****************************************************************************
 *
 * CREATE FUNCTION stmt
 *
  *****************************************************************************/
CreateFunctionStmt:
		CREATE_P OptTemp macro_alias qualified_name table_macro_list
			{
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				$4->relpersistence = $2;
				n->name = $4;
				n->functions = $5;
				n->onconflict = PG_ERROR_ON_CONFLICT;
				n->is_procedure = $3;
				$$ = (PGNode *)n;
			}
		| CREATE_P OptTemp macro_alias IF_P NOT EXISTS qualified_name table_macro_list
			{
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				$7->relpersistence = $2;
				n->name = $7;
				n->functions = $8;
				n->onconflict = PG_IGNORE_ON_CONFLICT;
				n->is_procedure = $3;
				$$ = (PGNode *)n;
			}
		| CREATE_P OR REPLACE OptTemp macro_alias qualified_name table_macro_list
			{
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				$6->relpersistence = $4;
				n->name = $6;
				n->functions = $7;
				n->onconflict = PG_REPLACE_ON_CONFLICT;
				n->is_procedure = $5;
				$$ = (PGNode *)n;
			}
		| CREATE_P OptTemp macro_alias qualified_name macro_definition_list  %prec CREATE_FUNC_BODY
			{
					PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
					$4->relpersistence = $2;
					n->name = $4;
					n->functions = $5;
					n->onconflict = PG_ERROR_ON_CONFLICT;
					n->is_procedure = $3;
					$$ = (PGNode *)n;
			}
		| CREATE_P OptTemp macro_alias IF_P NOT EXISTS qualified_name macro_definition_list  %prec CREATE_FUNC_BODY
			 {
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				$7->relpersistence = $2;
				n->name = $7;
				n->functions = $8;
				n->onconflict = PG_IGNORE_ON_CONFLICT;
				n->is_procedure = $3;
				$$ = (PGNode *)n;
			 }
		| CREATE_P OR REPLACE OptTemp macro_alias qualified_name macro_definition_list  %prec CREATE_FUNC_BODY
			 {
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				$6->relpersistence = $4;
				n->name = $6;
				n->functions = $7;
				n->onconflict = PG_REPLACE_ON_CONFLICT;
				n->is_procedure = $5;
				$$ = (PGNode *)n;
			 }
		/* PG-style trailing LANGUAGE: CREATE FUNCTION f() AS $$body$$ LANGUAGE SQL */
		| CREATE_P OptTemp macro_alias qualified_name macro_definition_list LANGUAGE SQL_P
			{
					PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
					$4->relpersistence = $2;
					n->name = $4;
					n->functions = $5;
					n->onconflict = PG_ERROR_ON_CONFLICT;
					n->is_procedure = $3;
					n->has_language = true;
					$$ = (PGNode *)n;
			}
		| CREATE_P OptTemp macro_alias IF_P NOT EXISTS qualified_name macro_definition_list LANGUAGE SQL_P
			 {
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				$7->relpersistence = $2;
				n->name = $7;
				n->functions = $8;
				n->onconflict = PG_IGNORE_ON_CONFLICT;
				n->is_procedure = $3;
				n->has_language = true;
				$$ = (PGNode *)n;
			 }
		| CREATE_P OR REPLACE OptTemp macro_alias qualified_name macro_definition_list LANGUAGE SQL_P
			 {
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				$6->relpersistence = $4;
				n->name = $6;
				n->functions = $7;
				n->onconflict = PG_REPLACE_ON_CONFLICT;
				n->is_procedure = $5;
				n->has_language = true;
				$$ = (PGNode *)n;
			 }
	;

table_macro_definition:
		param_list AS TABLE select_no_parens
			{
				PGFunctionDefinition *n = makeNode(PGFunctionDefinition);
				n->params = $1;
				n->query = $4;
				$$ = (PGNode *)n;
			}
		| param_list pg_function_decorators BEGIN_P ATOMIC select_no_parens ';' END_P
			{
				PGFunctionDefinition *n = makeNode(PGFunctionDefinition);
				n->params = $1;
				n->query = $5;
				n->returns_table_columns = $2;
				$$ = (PGNode *)n;
			}
		| param_list pg_function_decorators BEGIN_P ATOMIC select_no_parens END_P
			{
				PGFunctionDefinition *n = makeNode(PGFunctionDefinition);
				n->params = $1;
				n->query = $5;
				n->returns_table_columns = $2;
				$$ = (PGNode *)n;
			}
	;

table_macro_definition_parens:
		param_list AS TABLE select_with_parens
			{
				PGFunctionDefinition *n = makeNode(PGFunctionDefinition);
				n->params = $1;
				n->query = $4;
				$$ = (PGNode *)n;
			}
	;

table_macro_list_internal:
		table_macro_definition_parens
			{
				$$ = list_make1($1);
			}
		| table_macro_list_internal ',' table_macro_definition_parens
			{
				$$ = lappend($1, $3);
			}
	;

table_macro_list:
		table_macro_definition
			{
				$$ = list_make1($1);
			}
		| table_macro_list_internal
	;

macro_definition:
		param_list AS a_expr
			{
				PGFunctionDefinition *n = makeNode(PGFunctionDefinition);
				n->params = $1;
				n->function = $3;
				$$ = (PGNode *)n;
			}
		| param_list pg_function_decorators RETURN a_expr
			{
				PGFunctionDefinition *n = makeNode(PGFunctionDefinition);
				n->params = $1;
				n->function = $4;
				n->returns_table_columns = $2;
				$$ = (PGNode *)n;
			}
		| param_list pg_function_decorators AS Sconst
			{
				PGFunctionDefinition *n = makeNode(PGFunctionDefinition);
				n->params = $1;
				n->returns_table_columns = $2;
				n->has_language = true;
				/* Store body as a string constant expression -- parsed in transformer */
				PGAConst *c = makeNode(PGAConst);
				c->val.type = T_PGString;
				c->val.val.str = $4;
				c->location = @4;
				n->function = (PGNode *) c;
				$$ = (PGNode *)n;
			}
	;

macro_definition_list:
		macro_definition
			{
				$$ = list_make1($1);
			}
		| macro_definition_list ',' macro_definition
			{
				$$ = lappend($1, $3);
			}
	;

macro_alias:
		FUNCTION    { $$ = false; }
		| MACRO     { $$ = false; }
		| PROCEDURE { $$ = true; }
	;

/*****************************************************************************
 * PG-style function decorators (parsed and ignored)
 *****************************************************************************/
/* Returns PGList* with RETURNS TABLE columns (or NIL) */
pg_function_decorators:
		pg_function_decorator_list                           { $$ = $1; }
	;

pg_function_decorator_list:
		pg_function_decorator                                { $$ = $1; }
		| pg_function_decorator_list pg_function_decorator   { $$ = $1 ? $1 : $2; }
	;

pg_function_decorator:
		RETURNS Typename                                     { $$ = NIL; }
		| RETURNS TABLE '(' TableFuncElementList ')'         { $$ = $4; }
		| RETURNS NULL_P ON NULL_P INPUT_P                   { $$ = NIL; }
		| CALLED ON NULL_P INPUT_P                           { $$ = NIL; }
		| LANGUAGE ColId                                     { $$ = NIL; }
		| IMMUTABLE                                          { $$ = NIL; }
		| STABLE                                             { $$ = NIL; }
		| VOLATILE                                           { $$ = NIL; }
		| STRICT_P                                           { $$ = NIL; }
		| LEAKPROOF                                          { $$ = NIL; }
		| NOT LEAKPROOF                                      { $$ = NIL; }
		| PARALLEL ColId                                     { $$ = NIL; }
		| COST NumericOnly                                   { $$ = NIL; }
		| ROWS NumericOnly                                   { $$ = NIL; }
		| SECURITY INVOKER                                   { $$ = NIL; }
		| SECURITY DEFINER                                   { $$ = NIL; }
		| EXTERNAL SECURITY INVOKER                          { $$ = NIL; }
		| EXTERNAL SECURITY DEFINER                          { $$ = NIL; }
	;


param_list:
		'(' ')'
			{
				$$ = NIL;
			}
		| '(' MacroParameterList ',' ')'
			{
				$$ = $2;
			}
		| '(' MacroParameterList ')'
			{
				$$ = $2;
			}
	;

MacroParameterList:
		MacroParameter
			{
				$$ = list_make1($1);
			}
		| MacroParameterList ',' MacroParameter
			{
				$$ = lappend($1, $3);
			}
	;

/*
 * Function/macro parameter with optional default value.
 * Structure mirrors PG's func_arg_with_default / func_arg.
 *
 * PG syntax: [IN] [name] type [DEFAULT expr | = expr]
 *   - Type is always required. Bare type = unnamed positional param.
 *   - param_name uses type_function_name (subset of Typename start tokens).
 *   - FIRST(param_name) ⊂ FIRST(Typename) except GENERATED.
 *   - Bare Typename covers both PG unnamed type and DuckDB bare name.
 *     Transformer reinterprets GenericType as name when !has_language.
 *
 * DuckDB syntax: name [type] [:= expr | => expr]
 *   - Name is always present. Type is optional.
 *   - := and => disambiguate, so opt_Typename is safe here.
 *
 * To regenerate the missing-token list (GENERATED), run:
 *   python3 scripts/merge_grammar_rules_xml.py /tmp/grammar.xml \
 *       param_name_or_typename param_name Typename
 * after: bison --xml=/tmp/grammar.xml -o /tmp/g.cpp grammar.y.tmp
 */
MacroParameter:
		/* No default */
		PgFuncArg                                     { $$ = $1; }
		/*
		 * col_name_keywords usable as bare parameter names.
		 *
		 * param_name (= type_function_name) covers IDENT, unreserved, and
		 * type_func_name keywords. But some col_name_keywords are valid
		 * identifiers in PG yet missing from param_name. Most of them still
		 * work here because PgFuncArg falls through to the bare Typename rule
		 * (with ambiguous=true), and the transformer reinterprets GenericType
		 * as a name for DuckDB macros.
		 *
		 * The exceptions below are col_name_keywords that START a Typename
		 * production requiring mandatory '(' — e.g. MAP '(' type_list ')'.
		 * A bare MAP token begins the Typename match, the parser expects '(',
		 * sees ',' or ')' instead, and fails. So we list them here as direct
		 * name-only alternatives to bypass Typename matching.
		 *
		 * GENERATED is in param_name's FIRST set but not Typename's, so it
		 * also needs an explicit alternative.
		 *
		 * To regenerate this list after grammar changes:
		 *   bison --xml=/tmp/grammar.xml -o /dev/null grammar.y.tmp
		 *   python3 scripts/merge_grammar_rules_xml.py \
		 *       --nonbare-typename /tmp/grammar.xml \
		 *       third_party/libpg_query/grammar/keywords/column_name_keywords.list
		 */
		| GENERATED  { PGFunctionParameter *n = makeNode(PGFunctionParameter); n->name = pstrdup($1); $$ = (PGNode *) n; }
		| MAP        { PGFunctionParameter *n = makeNode(PGFunctionParameter); n->name = pstrdup($1); $$ = (PGNode *) n; }
		| STRUCT     { PGFunctionParameter *n = makeNode(PGFunctionParameter); n->name = pstrdup($1); $$ = (PGNode *) n; }
		| ROW        { PGFunctionParameter *n = makeNode(PGFunctionParameter); n->name = pstrdup($1); $$ = (PGNode *) n; }
		| SETOF      { PGFunctionParameter *n = makeNode(PGFunctionParameter); n->name = pstrdup($1); $$ = (PGNode *) n; }
		| NATIONAL   { PGFunctionParameter *n = makeNode(PGFunctionParameter); n->name = pstrdup($1); $$ = (PGNode *) n; }
		/* DuckDB defaults: name [type] := expr  |  name [type] => expr */
		| param_name opt_Typename COLON_EQUALS a_expr
			{
				PGFunctionParameter *n = makeNode(PGFunctionParameter);
				n->name = $1;
				n->typeName = $2;
				n->defaultValue = (PGExpr *) $4;
				$$ = (PGNode *) n;
			}
		| param_name opt_Typename EQUALS_GREATER a_expr
			{
				PGFunctionParameter *n = makeNode(PGFunctionParameter);
				n->name = $1;
				n->typeName = $2;
				n->defaultValue = (PGExpr *) $4;
				$$ = (PGNode *) n;
			}
		/* PG defaults: func_arg DEFAULT expr  |  func_arg = expr */
		| PgFuncArg DEFAULT a_expr
			{
				$$ = $1;
				((PGFunctionParameter *)$$)->defaultValue = (PGExpr *) $3;
			}
		| PgFuncArg '=' a_expr
			{
				$$ = $1;
				((PGFunctionParameter *)$$)->defaultValue = (PGExpr *) $3;
			}
	;

/* PG's func_arg: [IN] [name] type — 5 combinations, 0 conflicts */
PgFuncArg:
		IN_P param_name Typename
			{
				PGFunctionParameter *n = makeNode(PGFunctionParameter);
				n->name = $2;
				n->typeName = $3;
				$$ = (PGNode *) n;
			}
		| param_name IN_P Typename
			{
				PGFunctionParameter *n = makeNode(PGFunctionParameter);
				n->name = $1;
				n->typeName = $3;
				$$ = (PGNode *) n;
			}
		| param_name Typename
			{
				PGFunctionParameter *n = makeNode(PGFunctionParameter);
				n->name = $1;
				n->typeName = $2;
				$$ = (PGNode *) n;
			}
		| IN_P Typename
			{
				PGFunctionParameter *n = makeNode(PGFunctionParameter);
				n->typeName = $2;
				$$ = (PGNode *) n;
			}
		| Typename
			{
				PGFunctionParameter *n = makeNode(PGFunctionParameter);
				n->typeName = $1;
				n->ambiguous = true;
				/* Store name from Typename for DuckDB bare-name reinterpretation */
				if ($1->names && $1->names->length > 0) {
					n->name = strVal($1->names->tail->data.ptr_value);
				}
				$$ = (PGNode *) n;
			}
	;
