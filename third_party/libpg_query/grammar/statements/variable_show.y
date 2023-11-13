/* allows SET or RESET without LOCAL */
VariableShowStmt:
			show_or_describe SelectStmt {
				PGVariableShowSelectStmt *n = makeNode(PGVariableShowSelectStmt);
				n->stmt = $2;
				n->name = (char*) "select";
				n->is_summary = 0;
				$$ = (PGNode *) n;
			}
		 | SUMMARIZE SelectStmt {
				PGVariableShowSelectStmt *n = makeNode(PGVariableShowSelectStmt);
				n->stmt = $2;
				n->name = (char*) "select";
				n->is_summary = 1;
				$$ = (PGNode *) n;
			}
		 | SUMMARIZE table_id
			{
				PGVariableShowStmt *n = makeNode(PGVariableShowStmt);
				n->name = $2;
				n->is_summary = 1;
				$$ = (PGNode *) n;
			}
		 | show_or_describe table_id
			{
				PGVariableShowStmt *n = makeNode(PGVariableShowStmt);
				n->name = $2;
				n->is_summary = 0;
				$$ = (PGNode *) n;
			}
		| show_or_describe TIME ZONE
			{
				PGVariableShowStmt *n = makeNode(PGVariableShowStmt);
				n->name = (char*) "timezone";
				n->is_summary = 0;
				$$ = (PGNode *) n;
			}
		| show_or_describe TRANSACTION ISOLATION LEVEL
			{
				PGVariableShowStmt *n = makeNode(PGVariableShowStmt);
				n->name = (char*) "transaction_isolation";
				n->is_summary = 0;
				$$ = (PGNode *) n;
			}
		| show_or_describe ALL opt_tables
			{
				PGVariableShowStmt *n = makeNode(PGVariableShowStmt);
				n->name = (char*) "__show_tables_expanded";
				n->is_summary = 0;
				$$ = (PGNode *) n;
			}
		| show_or_describe
			{
				PGVariableShowStmt *n = makeNode(PGVariableShowStmt);
				n->name = (char*) "__show_tables_expanded";
				n->is_summary = 0;
				$$ = (PGNode *) n;
			}
		;

show_or_describe: SHOW | DESCRIBE

opt_tables: TABLES | /* empty */

var_name:	ColId								{ $$ = $1; }
			| var_name '.' ColId
				{ $$ = psprintf("%s.%s", $1, $3); }
		;

table_id:	ColId								{ $$ = psprintf("\"%s\"", $1); }
			| table_id '.' ColId
				{ $$ = psprintf("%s.\"%s\"", $1, $3); }
		;
