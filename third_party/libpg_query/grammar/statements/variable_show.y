/* allows SET or RESET without LOCAL */
VariableShowStmt:
			show_or_describe var_name
				{
					PGVariableShowStmt *n = makeNode(PGVariableShowStmt);
					n->name = $2;
					$$ = (PGNode *) n;
				}
			| show_or_describe TIME ZONE
				{
					PGVariableShowStmt *n = makeNode(PGVariableShowStmt);
					n->name = (char*) "timezone";
					$$ = (PGNode *) n;
				}
			| show_or_describe TRANSACTION ISOLATION LEVEL
				{
					PGVariableShowStmt *n = makeNode(PGVariableShowStmt);
					n->name = (char*) "transaction_isolation";
					$$ = (PGNode *) n;
				}
			| show_or_describe ALL
				{
					PGVariableShowStmt *n = makeNode(PGVariableShowStmt);
					n->name = (char*) "all";
					$$ = (PGNode *) n;
				}
		;

show_or_describe: SHOW | DESCRIBE

var_name:	ColId								{ $$ = $1; }
			| var_name '.' ColId
				{ $$ = psprintf("%s.%s", $1, $3); }
		;
