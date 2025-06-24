VariableResetStmt:
			RESET reset_rest
			{
				$2->scope = VAR_SET_SCOPE_DEFAULT;
				$$ = (PGNode *) $2;
			}
			| RESET LOCAL reset_rest
				{
					$3->scope = VAR_SET_SCOPE_LOCAL;
					$$ = (PGNode *) $3;
				}
			| RESET SESSION reset_rest
				{
					$3->scope = VAR_SET_SCOPE_SESSION;
					$$ = (PGNode *) $3;
				}
			| RESET GLOBAL reset_rest
				{
					$3->scope = VAR_SET_SCOPE_GLOBAL;
					$$ = (PGNode *) $3;
				}
			| RESET VARIABLE_P reset_rest
				{
					$3->scope = VAR_SET_SCOPE_VARIABLE;
					$$ = (PGNode *) $3;
				}
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
