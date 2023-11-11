/*****************************************************************************
 *
 * Create a secret
 *
 *****************************************************************************/
CreateSecretStmt:
			CREATE_P opt_persist SECRET opt_secret_name '(' secret_key_val ')'
				{
					PGCreateSecretStmt *n = makeNode(PGCreateSecretStmt);
					n->persist_option = $2;
					n->secret_name = $4;
					n->options = $6;
					n->onconflict = PG_ERROR_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
			| CREATE_P opt_persist SECRET IF_P NOT EXISTS opt_secret_name '(' secret_key_val ')'
				{
					PGCreateSecretStmt *n = makeNode(PGCreateSecretStmt);
					n->persist_option = $2;
					n->secret_name = $7;
					n->options = $9;
					n->onconflict = PG_IGNORE_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
			| CREATE_P OR REPLACE opt_persist SECRET opt_secret_name '(' secret_key_val ')'
				{
					PGCreateSecretStmt *n = makeNode(PGCreateSecretStmt);
					n->persist_option = $4;
					n->secret_name = $6;
					n->options = $8;
					n->onconflict = PG_REPLACE_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
		;

opt_secret_name:
	/* empty */ { $$ = NULL; }
	| ColId { $$ = $1; }
	;

/*------------------- WITH -------------------*/
secret_key:
		ColId SCONST                                { $$ = list_make2($1, makeString($2)); }
		| ColId ColId						        { $$ = list_make2($1, makeString($2)); }
		| ColId '[' scope_list_val ']'		        { $$ = list_make2($1, $3); }
	;

secret_key_val:
		secret_key						            { $$ = list_make1($1); }
		| secret_key_val ',' secret_key	            { $$ = lappend($1,$3); }
	;

/*------------------- Scope -------------------*/
scope_list_val_item:
		SCONST                                      { $$ = makeString($1); }
	;

scope_list_val:
		scope_list_val_item						    { $$ = list_make1($1); }
		| scope_list_val ',' scope_list_val_item	{ $$ = lappend($1,$3); }
    ;

/*------------------- Scope -------------------*/
opt_persist:
        /* empty */                                 { $$ = "default"; }
        | TEMPORARY                                 { $$ = "temporary"; }
        | PERMANENT                                 { $$ = "permanent"; }
    ;

