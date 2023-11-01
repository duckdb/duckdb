/*****************************************************************************
 *
 * Create a secret
 *
 *****************************************************************************/
CreateSecretStmt:
			CREATE_P SECRET ColId TYPE_P ColId opt_provider_val opt_scope_val opt_secret_list
				{
					PGCreateSecretStmt *n = makeNode(PGCreateSecretStmt);
					n->secret_name = $3;
					n->secret_type = $5;
					n->secret_provider = $6;
					n->scope = $7;
					n->options = $8;
					n->onconflict = PG_ERROR_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
			| CREATE_P SECRET IF_P NOT EXISTS ColId TYPE_P ColId opt_provider_val opt_scope_val opt_secret_list
				{
					PGCreateSecretStmt *n = makeNode(PGCreateSecretStmt);
					n->secret_name = $6;
					n->secret_type = $8;
					n->secret_provider = $9;
					n->scope = $10;
					n->options = $11;
					n->onconflict = PG_IGNORE_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
			| CREATE_P OR REPLACE SECRET ColId TYPE_P ColId opt_provider_val opt_scope_val opt_secret_list
				{
					PGCreateSecretStmt *n = makeNode(PGCreateSecretStmt);
					n->secret_name = $5;
					n->secret_type = $7;
					n->secret_provider = $8;
					n->scope = $9;
					n->options = $10;
					n->onconflict = PG_REPLACE_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
		;

/*------------------- WITH -------------------*/
secret_key:
		ColId SCONST                        { $$ = list_make2($1, $2); }
	;

secret_key_val:
		secret_key						    { $$ = list_make1($1); }
		| secret_key_val ',' secret_key	    { $$ = lappend($1,$3); }
	;

opt_secret_key_val:
		/* empty */						    { $$ = NULL; }
		| secret_key_val				    { $$ = $1; }
	;

opt_secret_list:
		/* empty */						    { $$ = NULL; }
		| WITH '(' opt_secret_key_val ')'   { $$ = $3; }
	;

/*------------------- Provider -------------------*/
provider_val:
		USING ColId			            { $$ = $2; }
	;

opt_provider_val:
		/* empty */						    { $$ = NULL; }
		| provider_val		                { $$ = $1; }
	;

/*------------------- Scope -------------------*/
scope_list_val_item:
		SCONST                                      { $$ = $1; }
	;

scope_list_val:
		scope_list_val_item						    { $$ = list_make1($1); }
		| scope_list_val ',' scope_list_val_item	{ $$ = lappend($1,$3); }
    ;

opt_scope_val:
        /* empty */						            { $$ = NULL; }
        | SCOPE '[' scope_list_val ']'              { $$ = $3; }
        | SCOPE scope_list_val_item       			{ $$ = list_make1($2); }
    ;

