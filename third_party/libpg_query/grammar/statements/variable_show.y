/* allows SET or RESET without LOCAL */
VariableShowStmt:
			SHOW var_name
				{
					VariableShowStmt *n = makeNode(VariableShowStmt);
					n->name = $2;
					$$ = (Node *) n;
				}
			| SHOW TIME ZONE
				{
					VariableShowStmt *n = makeNode(VariableShowStmt);
					n->name = (char*) "timezone";
					$$ = (Node *) n;
				}
			| SHOW TRANSACTION ISOLATION LEVEL
				{
					VariableShowStmt *n = makeNode(VariableShowStmt);
					n->name = (char*) "transaction_isolation";
					$$ = (Node *) n;
				}
			| SHOW ALL
				{
					VariableShowStmt *n = makeNode(VariableShowStmt);
					n->name = (char*) "all";
					$$ = (Node *) n;
				}
		;


var_name:	ColId								{ $$ = $1; }
			| var_name '.' ColId
				{ $$ = psprintf("%s.%s", $1, $3); }
		;
