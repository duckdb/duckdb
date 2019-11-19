VariableResetStmt:
			RESET reset_rest						{ $$ = (Node *) $2; }
		;


generic_reset:
			var_name
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET;
					n->name = $1;
					$$ = n;
				}
			| ALL
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET_ALL;
					$$ = n;
				}
		;


reset_rest:
			generic_reset							{ $$ = $1; }
			| TIME ZONE
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET;
					n->name = (char*) "timezone";
					$$ = n;
				}
			| TRANSACTION ISOLATION LEVEL
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET;
					n->name = (char*) "transaction_isolation";
					$$ = n;
				}
		;
