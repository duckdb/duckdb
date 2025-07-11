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
		 | SUMMARIZE qualified_name
			{
				PGVariableShowStmt *n = makeNode(PGVariableShowStmt);
				n->relation = $2;
				n->is_summary = 1;
				$$ = (PGNode *) n;
			}
		 | show_or_describe TABLES FROM qualified_name
			{
				PGVariableShowStmt *n = makeNode(PGVariableShowStmt);
				n->relation = $4;
				n->set = (char*) "__show_tables_from_database";
				n->is_summary = 0;
				$$ = (PGNode *) n;
			}
		| show_or_describe qualified_name
			{
				PGVariableShowStmt *n = makeNode(PGVariableShowStmt);
				n->relation = $2;
				n->is_summary = 0;
				$$ = (PGNode *) n;
			}
		| show_or_describe TIME ZONE
			{
				PGVariableShowStmt *n = makeNode(PGVariableShowStmt);
				n->set = (char*) "timezone";
				n->is_summary = 0;
				$$ = (PGNode *) n;
			}
		| show_or_describe TRANSACTION ISOLATION LEVEL
			{
				PGVariableShowStmt *n = makeNode(PGVariableShowStmt);
				n->set = (char*) "transaction_isolation";
				n->is_summary = 0;
				$$ = (PGNode *) n;
			}
		| show_or_describe ALL opt_tables
			{
				PGVariableShowStmt *n = makeNode(PGVariableShowStmt);
				n->set = (char*) "__show_tables_expanded";
				n->is_summary = 0;
				$$ = (PGNode *) n;
			}
		| show_or_describe
			{
				PGVariableShowStmt *n = makeNode(PGVariableShowStmt);
				n->set = (char*) "__show_tables_expanded";
				n->is_summary = 0;
				$$ = (PGNode *) n;
			}
		;

describe_or_desc: DESCRIBE | DESC_P

show_or_describe: SHOW | describe_or_desc

opt_tables: TABLES | /* empty */

var_name:	ColId								{ $$ = $1; }
			| var_name '.' ColId
				{ $$ = psprintf("%s.%s", $1, $3); }
		;
