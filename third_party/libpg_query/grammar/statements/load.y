/*****************************************************************************
 *
 *		QUERY:
 *				LOAD "filename"
 *
 *****************************************************************************/
LoadStmt:	LOAD file_name
				{
					PGLoadStmt *n = makeNode(PGLoadStmt);
					n->filename = $2;
					n->repository = NULL;
					n->repo_is_alias = false;
					n->version = NULL;
					n->load_type = PG_LOAD_TYPE_LOAD;
					$$ = (PGNode *)n;
				} |
				opt_force INSTALL file_name opt_ext_version {
                    PGLoadStmt *n = makeNode(PGLoadStmt);
                    n->filename = $3;
                    n->repository = NULL;
                    n->repo_is_alias = false;
                    n->version = $4;
                    n->load_type = $1;
                    $$ = (PGNode *)n;
				} |
				opt_force INSTALL file_name FROM ColId opt_ext_version {
                    PGLoadStmt *n = makeNode(PGLoadStmt);
                    n->repository = $5;
                    n->repo_is_alias = true;
                    n->filename = $3;
                    n->version = $6;
                    n->load_type = $1;
                    $$ = (PGNode *)n;
				} |
				opt_force INSTALL file_name FROM Sconst opt_ext_version {
                    PGLoadStmt *n = makeNode(PGLoadStmt);
                    n->filename = $3;
                    n->repository = $5;
                    n->repo_is_alias = false;
                    n->version = $6;
                    n->load_type = $1;
                    $$ = (PGNode *)n;
				}
		;

opt_force:	/* empty*/							{ $$ = PG_LOAD_TYPE_INSTALL; } |
            FORCE                               { $$ = PG_LOAD_TYPE_FORCE_INSTALL; };

file_name:	Sconst								{ $$ = $1; } |
            ColId                               { $$ = $1; };

opt_ext_version:
               /* empty */                      { $$ = NULL; } |
                VERSION_P Sconst				{ $$ = $2; } |
                VERSION_P ColId                 { $$ = $2; };
