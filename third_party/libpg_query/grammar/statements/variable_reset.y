VariableResetStmt:
			RESET reset_rest						{ $$ = (PGNode *) $2; }
		;


generic_reset:
			var_name
				{
					PGVariableSetStmt *n = makeNode(PGVariableSetStmt);
					n->kind = VAR_RESET;
					n->name = $1;
					$$ = n;
				}
			| ALL
				{
					PGVariableSetStmt *n = makeNode(PGVariableSetStmt);
					n->kind = VAR_RESET_ALL;
					$$ = n;
				}
		;


reset_rest:
			generic_reset							{ $$ = $1; }
			| TIME ZONE
				{
					PGVariableSetStmt *n = makeNode(PGVariableSetStmt);
					n->kind = VAR_RESET;
					n->name = (char*) "timezone";
					$$ = n;
				}
			| TRANSACTION ISOLATION LEVEL
				{
					PGVariableSetStmt *n = makeNode(PGVariableSetStmt);
					n->kind = VAR_RESET;
					n->name = (char*) "transaction_isolation";
					$$ = n;
				}
		;
