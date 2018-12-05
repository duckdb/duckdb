/*
** Copyright (c) 2008 D. Richard Hipp
**
** This program is free software; you can redistribute it and/or
** modify it under the terms of the GNU General Public
** License version 2 as published by the Free Software Foundation.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
** General Public License for more details.
**
** You should have received a copy of the GNU General Public
** License along with this library; if not, write to the
** Free Software Foundation, Inc., 59 Temple Place - Suite 330,
** Boston, MA  02111-1307, USA.
**
** Author contact information:
**   drh@hwaci.com
**   http://www.hwaci.com/drh/
**
*******************************************************************************
**
** This module defines the interfaces to the sqllogictest program.
*/

/*
** The interface to each database engine is an instance of the
** following structure.
*/
typedef struct DbEngine DbEngine;
struct DbEngine {
	const char *zName; /* Name of this engine */
	void *pAuxData;    /* Aux data passed to xConnect */
	int (*xConnect)(void *, const char *zCon, void **ppConn, const char *zOpt);
	int (*xGetEngineName)(void *, const char **zName);
	int (*xStatement)(void *, const char *zSql, int bQuiet); /* True to suppress printing errors. */
	int (*xQuery)(void *, const char *zSql, const char *zTypes, char ***pazResult, int *pnResult);
	int (*xFreeResults)(void *, char **azResult, int nResult);
	int (*xDisconnect)(void *);
};

/*
** Each database engine interface invokes the following routine
** to register itself with the main sqllogictest driver.
*/
void sqllogictestRegisterEngine(const DbEngine *);

/*
** MD5 hashing routines.
*/
void md5_add(const char *z);
const char *md5_finish(void);
