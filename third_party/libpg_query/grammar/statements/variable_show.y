/* allows SET or RESET without LOCAL */
VariableShowStmt:
			SHOW var_name
				{
					PGVariableShowStmt *n = makeNode(PGVariableShowStmt);
					n->name = $2;
					$$ = (PGNode *) n;
				}
			| SHOW TIME ZONE
				{
					PGVariableShowStmt *n = makeNode(PGVariableShowStmt);
					n->name = (char*) "timezone";
					$$ = (PGNode *) n;
				}
			| SHOW TRANSACTION ISOLATION LEVEL
				{
					PGVariableShowStmt *n = makeNode(PGVariableShowStmt);
					n->name = (char*) "transaction_isolation";
					$$ = (PGNode *) n;
				}
			| SHOW ALL
				{
					PGVariableShowStmt *n = makeNode(PGVariableShowStmt);
					n->name = (char*) "all";
					$$ = (PGNode *) n;
				}
		;


var_name:	ColId								{ $$ = $1; }
			| var_name '.' ColId
				{ $$ = psprintf("%s.%s", $1, $3); }
		;
