/*****************************************************************************
 *
 * Create a secret
 *
 *****************************************************************************/
CreateSecretStmt:
			CREATE_P SECRET ColId TYPE_P '=' ColId opt_secret_key_val
				{
					PGCreateSecretStmt *n = makeNode(PGCreateSecretStmt);
					n->secret_name = $3;
					n->secret_type = $6;
					n->options = $7;
					n->onconflict = PG_ERROR_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
			| CREATE_P SECRET IF_P NOT EXISTS ColId TYPE_P '=' ColId opt_secret_key_val
				{
					PGCreateSecretStmt *n = makeNode(PGCreateSecretStmt);
					n->secret_name = $6;
					n->secret_type = $9;
					n->options = $10;
					n->onconflict = PG_IGNORE_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
			| CREATE_P OR REPLACE SECRET ColId TYPE_P '=' ColId opt_secret_key_val
				{
					PGCreateSecretStmt *n = makeNode(PGCreateSecretStmt);
					n->secret_name = $5;
					n->secret_type = $8;
					n->options = $9;
					n->onconflict = PG_REPLACE_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
		;

secret_key:
		ColId '=' SCONST				{ $$ = list_make2($1, $3); }
	;

secret_key_val:
		secret_key						{ $$ = list_make1($1); }
		| secret_key_val secret_key	    { $$ = lappend($1,$2); }
	;

opt_secret_key_val:
		/* empty */						{ $$ = NULL; }
		| secret_key_val				{ $$ = $1; }
	;


