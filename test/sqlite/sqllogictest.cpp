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
** This main driver for the sqllogictest program.
*/
#include "catch.hpp"

#include "sqllogictest.hpp"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#ifndef _WIN32
#include <unistd.h>
#define stricmp strcasecmp
#endif
#include <string.h>

#include <string>
#include <vector>

#include "slt_duckdb.hpp"

using namespace std;

#define DEFAULT_HASH_THRESHOLD 8

/*
** An array of registered database engines
*/
static int nEngine = 0;
static const DbEngine **apEngine = 0;

/*
** Register a new database engine.
*/
void sqllogictestRegisterEngine(const DbEngine *p) {
	nEngine++;
	apEngine =
	    (const DbEngine **)realloc(apEngine, nEngine * sizeof(apEngine[0]));
	if (apEngine == 0) {
		fprintf(stderr, "out of memory at %s:%d\n", __FILE__, __LINE__);
		exit(1);
	}
	apEngine[nEngine - 1] = (DbEngine *)p;
}

/*
** Print a usage comment and die
*/
// static void usage(const char *argv0) {
// 	fprintf(stdout, "Usage: %s [options] script\n", argv0);
// 	fprintf(
// 	    stdout,
// 	    "Options:\n"
// 	    "  --connection STR       The connection string\n"
// 	    "  --engine DBENGINE      The engine name (ex: SQLite, ODBC3)\n"
// 	    "  --halt                 Stop when first error is seen\n"
// 	    "  --ht NUM               Check results by hash if numbe of lines > "
// 	    "NUM\n"
// 	    "  --odbc STR             Shorthand for \"--engine ODBC3 --connection "
// 	    "STR\"\n"
// 	    "  --parameters TXT       Extra parameters to the connection string\n"
// 	    "  --trace                Enable tracing of SQL to standard output\n"
// 	    "  --verify               Use \"verify MODE\"\n");
// 	exit(1);
// }

/*
** A structure to keep track of the state of scanning the input script.
*/
typedef struct Script Script;
struct Script {
	char *zScript;        /* Complete text of the input script */
	int iCur;             /* Index in zScript of start of current line */
	char *zLine;          /* Pointer to start of current line */
	int len;              /* Length of current line */
	int iNext;            /* index of start of next line */
	int nLine;            /* line number for the current line */
	int iEnd;             /* Index in zScript of '\000' at end of script */
	int startLine;        /* Line number of start of current record */
	int copyFlag;         /* If true, copy lines to output as they are read */
	char azToken[4][200]; /* tokenization of a line */
};

/*
** Advance the cursor to the start of the next non-comment line of the
** script.  Make p->zLine point to the start of the line.  Make p->len
** be the length of the line.  Zero-terminate the line.  Any \r at the
** end of the line is removed.
**
** Return 1 on success.  Return 0 and no-op at end-of-file.
*/
static int nextLine(Script *p) {
	int i;

	/* Loop until a non-comment line is found, or until end-of-file */
	while (1) {

		/* When we reach end-of-file, return 0 */
		if (p->iNext >= p->iEnd) {
			p->iCur = p->iEnd;
			p->zLine = &p->zScript[p->iEnd];
			p->len = 0;
			return 0;
		}

		/* Advance the cursor to the next line */
		p->iCur = p->iNext;
		p->nLine++;
		p->zLine = &p->zScript[p->iCur];
		for (i = p->iCur; i < p->iEnd && p->zScript[i] != '\n'; i++) {
		}
		p->zScript[i] = 0;
		p->len = i - p->iCur;
		p->iNext = i + 1;

		/* If the current line ends in a \r then remove the \r. */
		if (p->len > 0 && p->zScript[i - 1] == '\r') {
			p->len--;
			i--;
			p->zScript[i] = 0;
		}

		/* If the line consists of all spaces, make it an empty line */
		for (i = i - 1; i >= p->iCur && isspace(p->zScript[i]); i--) {
		}
		if (i < p->iCur) {
			p->zLine[0] = 0;
		}

		/* If the copy flag is set, write the line to standard output */
		if (p->copyFlag) {
			printf("%s\n", p->zLine);
		}

		/* If the line is not a comment line, then we are finished, so break
		** out of the loop.  If the line is a comment, the loop will repeat in
		** order to skip this line. */
		if (p->zLine[0] != '#')
			break;
	}
	return 1;
}

/*
** Look ahead to the next line and return TRUE if it is a blank line.
** But do not advance to the next line yet.
*/
static int nextIsBlank(Script *p) {
	int i = p->iNext;
	if (i >= p->iEnd)
		return 1;
	while (i < p->iEnd && isspace(p->zScript[i])) {
		if (p->zScript[i] == '\n')
			return 1;
		i++;
	}
	return 0;
}

/*
** Advance the cursor to the start of the next record.  To do this,
** first skip over the tail section of the record in which we are
** currently located, then skip over blank lines.
**
** Return 1 on success.  Return 0 at end-of-file.
*/
static int findStartOfNextRecord(Script *p) {

	/* Skip over any existing content to find a blank line */
	if (p->iCur > 0) {
		while (p->zLine[0] && p->iCur < p->iEnd) {
			nextLine(p);
		}
	} else {
		nextLine(p);
	}

	/* Skip over one or more blank lines to find the first line of the
	** new record */
	while (p->zLine[0] == 0 && p->iCur < p->iEnd) {
		nextLine(p);
	}

	/* Return 1 if we have not reached end of file. */
	return p->iCur < p->iEnd;
}

/*
** Find a single token in a string.  Return the index of the start
** of the token and the length of the token.
*/
static void findToken(const char *z, int *piStart, int *pLen) {
	int i;
	int iStart;
	for (i = 0; isspace(z[i]); i++) {
	}
	*piStart = iStart = i;
	while (z[i] && !isspace(z[i])) {
		i++;
	}
	*pLen = i - iStart;
}

#define count(X) (sizeof(X) / sizeof(X[0]))

/*
** tokenize the current line in up to 3 tokens and store those values
** into p->azToken[0], p->azToken[1], and p->azToken[2].  Record the
** current line in p->startLine.
*/
static void tokenizeLine(Script *p) {
	int i, j, k;
	int len, n;
	for (i = 0; i < (int)count(p->azToken); i++)
		p->azToken[i][0] = 0;
	p->startLine = p->nLine;
	for (i = j = 0; j < p->len && i < (int)count(p->azToken); i++) {
		findToken(&p->zLine[j], &k, &len);
		j += k;
		n = len;
		if (n >= (int)sizeof(p->azToken[0])) {
			n = sizeof(p->azToken[0]) - 1;
		}
		memcpy(p->azToken[i], &p->zLine[j], n);
		p->azToken[i][n] = 0;
		j += n + 1;
	}
}

/*
** The number columns in a row of the current result set
*/
static int nColumn = 0;

/*
** Comparison function for sorting the result set.
*/
static int rowCompare(const void *pA, const void *pB) {
	const char **azA = (const char **)pA;
	const char **azB = (const char **)pB;
	int c = 0, i;
	for (i = 0; c == 0 && i < nColumn; i++) {
		c = strcmp(azA[i], azB[i]);
	}
	return c;
}

/*
** Entry in a hash table of prior results
*/
typedef struct HashEntry HashEntry;
struct HashEntry {
	char zKey[24];    /* The search key */
	char zHash[33];   /* The hash value stored */
	HashEntry *pNext; /* Next with same hash */
	HashEntry *pAll;  /* Next overall */
};

/*
** The hash table
*/
#define NHASH 1009
static HashEntry *aHash[NHASH];
static HashEntry *pAll;

/*
** Try to look up the value zKey in the hash table.  If the value
** does not exist, create it and return 0.  If the value does already
** exist return 0 if hash matches and 1 if the hash is different.
*/
static int checkValue(const char *zKey, const char *zHash) {
	unsigned int h;
	HashEntry *p;
	unsigned int i;

	h = 0;
	for (i = 0; zKey[i] && i < sizeof(p->zKey); i++) {
		h = h << 3 ^ h ^ zKey[i];
	}
	h = h % NHASH;
	for (p = aHash[h]; p; p = p->pNext) {
		if (strcmp(p->zKey, zKey) == 0) {
			return strcmp(p->zHash, zHash) != 0;
		}
	}
	p = (HashEntry *)malloc(sizeof(*p));
	if (p == 0) {
		fprintf(stderr, "out of memory at %s:%d\n", __FILE__, __LINE__);
		exit(1);
	}
	for (i = 0; zKey[i] && i < sizeof(p->zKey) - 1; i++) {
		p->zKey[i] = zKey[i];
	}
	p->zKey[i] = 0;
	for (i = 0; zHash[i] && i < sizeof(p->zHash) - 1; i++) {
		p->zHash[i] = zHash[i];
	}
	p->zHash[i] = 0;
	p->pAll = pAll;
	pAll = p;
	p->pNext = aHash[h];
	aHash[h] = p;
	return 0;
}

/*
** This is the main routine.  This routine runs first.  It processes
** command-line arguments then runs the test.
*/
TEST_CASE("[SLOW] Test SQLite Logic Test", "[sqlitelogic]") {
	int verifyMode = 0;               /* True if in -verify mode */
	int haltOnError = 0;              /* Stop on first error if true */
	int enableTrace = 0;              /* Trace SQL statements if true */
	const char *zScriptFile = 0;      /* Input script filename */
	const char *zDbEngine = "SQLite"; /* Name of database engine */
	const char *zConnection = 0;      /* Connection string on DB engine */
	const DbEngine *pEngine = 0;      /* Pointer to DbEngine object */
	int i;                            /* Loop counter */
	char *zScript;                    /* Content of the script */
	long nScript;                     /* Size of the script in bytes */
	long nGot;                        /* Number of bytes read */
	void *pConn;                      /* Connection to the database engine */
	int rc;                           /* Result code from subroutine call */
	int nErr = 0;                     /* Number of errors */
	int nCmd = 0;                     /* Number of SQL statements processed */
	int nSkipped = 0;                 /* Number of SQL statements skipped */
	int nResult;                      /* Number of query results */
	char **azResult;                  /* Query result vector */
	Script sScript;                   /* Script parsing status */
	FILE *in;                         /* For reading script */
	char zHash[100];                  /* Storage space for hash results */
	int hashThreshold = DEFAULT_HASH_THRESHOLD; /* Threshold for hashing res */
	int bHt = 0;            /* True if -ht command-line option */
	const char *zParam = 0; /* Argument to -parameters */

	registerDuckdb();

	if (zDbEngine == NULL) {
		zDbEngine = apEngine[0]->zName;
	}

	/* Scan the command-line and process arguments
	 */
	verifyMode = 1;
	zDbEngine = "DuckDB";
	/* Check for errors and missing arguments.  Find the database engine
	** to use for this run.
	*/
	for (i = 0; i < nEngine; i++) {
		if (stricmp(zDbEngine, apEngine[i]->zName) == 0) {
			pEngine = apEngine[i];
			break;
		}
	}
	REQUIRE(pEngine);
	/*
	** Read the entire script file contents into memory
	*/

	vector<string> files = {
	    "third_party/sqllogictest/test/select1.test",
	    "third_party/sqllogictest/test/select2.test",
	    "third_party/sqllogictest/test/select3.test",
	    "third_party/sqllogictest/test/random/select/slt_good_0.test",
	    "third_party/sqllogictest/test/random/select/slt_good_1.test",
	    "third_party/sqllogictest/test/random/select/slt_good_2.test",
	    "third_party/sqllogictest/test/random/select/slt_good_3.test",
	    "third_party/sqllogictest/test/random/select/slt_good_4.test",
	    "third_party/sqllogictest/test/random/select/slt_good_5.test",
	    "third_party/sqllogictest/test/random/select/slt_good_6.test",
	    "third_party/sqllogictest/test/random/select/slt_good_7.test",
	    "third_party/sqllogictest/test/random/select/slt_good_8.test",
	    "third_party/sqllogictest/test/random/select/slt_good_9.test",
	    "third_party/sqllogictest/test/random/select/slt_good_10.test",
	    "third_party/sqllogictest/test/random/select/slt_good_11.test",
	    "third_party/sqllogictest/test/random/select/slt_good_12.test",
	    "third_party/sqllogictest/test/random/select/slt_good_13.test",
	    "third_party/sqllogictest/test/random/select/slt_good_14.test",
	    "third_party/sqllogictest/test/random/select/slt_good_15.test",
	    "third_party/sqllogictest/test/random/select/slt_good_16.test",
	    "third_party/sqllogictest/test/random/select/slt_good_17.test",
	    "third_party/sqllogictest/test/random/select/slt_good_18.test",
	    "third_party/sqllogictest/test/random/select/slt_good_19.test",
	    "third_party/sqllogictest/test/random/select/slt_good_20.test",
	    "third_party/sqllogictest/test/random/select/slt_good_21.test"};

	for (auto &script : files) {
		zScriptFile = script.c_str();
		in = fopen(zScriptFile, "rb");
		if (!in) {
			FAIL("Could not find test script '" + script +
			     "'. Perhaps run `make sqlite`. ");
		}
		REQUIRE(in);
		fseek(in, 0L, SEEK_END);
		nScript = ftell(in);
		zScript = (char *)malloc(nScript + 1);
		REQUIRE(zScript);
		fseek(in, 0L, SEEK_SET);
		nGot = fread(zScript, 1, nScript, in);
		fclose(in);
		REQUIRE(nGot >= nScript);
		zScript[nGot] = 0;

		// zap hash table as result labels are only valid within one test file
		memset(aHash, 0, sizeof(aHash));

		/* Initialize the sScript structure so that the cursor will be pointing
		** to the start of the first line in the file after nextLine() is called
		** once. */
		memset(&sScript, 0, sizeof(sScript));
		sScript.zScript = zScript;
		sScript.zLine = zScript;
		sScript.iEnd = nScript;
		sScript.copyFlag = !verifyMode;

		/* Open the database engine under test
		 */
		rc = pEngine->xConnect(pEngine->pAuxData, zConnection, &pConn, zParam);
		REQUIRE(rc == 0);

		/* Get the "real" db name
		 */
		rc = pEngine->xGetEngineName(pConn, &zDbEngine);
		REQUIRE(rc == 0);

		/* Loop over all records in the file */
		while ((nErr == 0 || !haltOnError) && findStartOfNextRecord(&sScript)) {
			int bSkip = 0; /* True if we should skip the current record. */

			/* Tokenizer the first line of the record.  This also records the
			** line number of the first record in sScript.startLine */
			tokenizeLine(&sScript);

			bSkip = 0;
			while (strcmp(sScript.azToken[0], "skipif") == 0 ||
			       strcmp(sScript.azToken[0], "onlyif") == 0) {
				int bMatch;
				/* The "skipif" and "onlyif" modifiers allow skipping or using
				** statement or query record for a particular database engine.
				** In this way, SQL features implmented by a majority of the
				** engines can be tested without causing spurious errors for
				** engines that don't support it.
				**
				** Once this record is encountered, an the current selected
				** db interface matches the db engine specified in the record,
				** the we skip this rest of this record for "skipif" or for
				** "onlyif" we skip the record if the record does not match.
				*/
				bMatch = stricmp(sScript.azToken[1], zDbEngine) == 0;
				if (sScript.azToken[0][0] == 's') {
					if (bMatch)
						bSkip = -1;
				} else {
					if (!bMatch)
						bSkip = -1;
				}
				nextLine(&sScript);
				tokenizeLine(&sScript);
			}
			if (bSkip) {
				int n;
				nSkipped++;
				if (!verifyMode)
					continue;
				if (strcmp(sScript.azToken[0], "query") != 0)
					continue;
				if (sScript.azToken[3][0] == 0)
					continue;

				/* We are skipping this record.  But we observe that it is a
				 *query
				 ** with a named hash value and we are in verify mode.  Even
				 *though
				 ** we are going to skip the SQL evaluation, we might as well
				 *check
				 ** the hash of the result.
				 */
				while (!nextIsBlank(&sScript) && nextLine(&sScript) &&
				       strcmp(sScript.zLine, "----") != 0) {
					/* Skip over the SQL text */
				}
				if (strcmp(sScript.zLine, "----") == 0)
					nextLine(&sScript);
				if (sScript.zLine[0] == 0)
					continue;
				n = sscanf(sScript.zLine, "%*d values hashing to %32s", zHash);
				if (n != 1) {
					md5_add(sScript.zLine);
					md5_add("\n");
					while (!nextIsBlank(&sScript) && nextLine(&sScript)) {
						md5_add(sScript.zLine);
						md5_add("\n");
					}
					strcpy(zHash, md5_finish());
				}
				if (checkValue(sScript.azToken[3], zHash)) {
					fprintf(stderr,
					        "%s:%d: labeled result [%s] does not agree with "
					        "previous values\n",
					        zScriptFile, sScript.startLine, sScript.azToken[3]);
					REQUIRE(false);
				}
				continue;
			}

			/* Figure out the record type and do appropriate processing */
			if (strcmp(sScript.azToken[0], "statement") == 0) {
				int k = 0;
				int bExpectOk = 0;
				int bExpectError = 0;

				/* Extract the SQL from second and subsequent lines of the
				** record.  Copy the SQL into contiguous memory at the beginning
				** of zScript - we are guaranteed to have enough space there. */
				while (nextLine(&sScript) && sScript.zLine[0]) {
					if (k > 0)
						zScript[k++] = '\n';
					memmove(&zScript[k], sScript.zLine, sScript.len);
					k += sScript.len;
				}
				zScript[k] = 0;

				bExpectOk = strcmp(sScript.azToken[1], "ok") == 0;
				bExpectError = strcmp(sScript.azToken[1], "error") == 0;

				/* Run the statement.  Remember the results
				** If we're expecting an error, pass true to suppress
				** printing of any errors.
				*/
				if (enableTrace)
					printf("%s;\n", zScript);
				rc = pEngine->xStatement(pConn, zScript, bExpectError);
				nCmd++;

				/* Check to see if we are expecting success or failure */
				if (bExpectOk) {
					/* do nothing if we expect success */
				} else if (bExpectError) {
					/* Invert the result if we expect failure */
					rc = !rc;
				} else {
					fprintf(
					    stderr,
					    "%s:%d: statement argument should be 'ok' or 'error'\n",
					    zScriptFile, sScript.startLine);
					REQUIRE(false);
					rc = 0;
				}

				/* Report an error if the results do not match expectation */
				REQUIRE(!rc);
				if (rc) {
					fprintf(stderr, "%s:%d: statement error\n", zScriptFile,
					        sScript.startLine);
					REQUIRE(false);
				}
			} else if (strcmp(sScript.azToken[0], "query") == 0) {
				int k = 0;
				int c;

				/* Verify that the type string consists of one or more
				 *characters
				 ** from the set "TIR". */
				for (k = 0; (c = sScript.azToken[1][k]) != 0; k++) {
					if (c != 'T' && c != 'I' && c != 'R') {
						fprintf(stderr,
						        "%s:%d: unknown type character '%c' in type "
						        "string\n",
						        zScriptFile, sScript.startLine, c);
						nErr++;
						break;
					}
				}
				if (c != 0)
					continue;
				if (k <= 0) {
					fprintf(stderr, "%s:%d: missing type string\n", zScriptFile,
					        sScript.startLine);
					REQUIRE(false);
					break;
				}

				/* Extract the SQL from second and subsequent lines of the
				 *record
				 ** until the first "----" line or until end of record.
				 */
				k = 0;
				while (!nextIsBlank(&sScript) && nextLine(&sScript) &&
				       sScript.zLine[0] && strcmp(sScript.zLine, "----") != 0) {
					if (k > 0)
						zScript[k++] = '\n';
					memmove(&zScript[k], sScript.zLine, sScript.len);
					k += sScript.len;
				}
				zScript[k] = 0;

				/* Run the query */
				nResult = 0;
				azResult = 0;
				if (enableTrace)
					printf("%s;\n", zScript);
				rc = pEngine->xQuery(pConn, zScript, sScript.azToken[1],
				                     &azResult, &nResult);
				nCmd++;
				if (rc) {
					fprintf(stderr, "%s:%d: query failed\n", zScriptFile,
					        sScript.startLine);
					pEngine->xFreeResults(pConn, azResult, nResult);
					REQUIRE(false);
					continue;
				}

				/* Do any required sorting of query results */
				if (sScript.azToken[2][0] == 0 ||
				    strcmp(sScript.azToken[2], "nosort") == 0) {
					/* Do no sorting */
				} else if (strcmp(sScript.azToken[2], "rowsort") == 0) {
					/* Row-oriented sorting */
					nColumn = (int)strlen(sScript.azToken[1]);
					qsort(azResult, nResult / nColumn,
					      sizeof(azResult[0]) * nColumn, rowCompare);
				} else if (strcmp(sScript.azToken[2], "valuesort") == 0) {
					/* Sort all values independently */
					nColumn = 1;
					qsort(azResult, nResult, sizeof(azResult[0]), rowCompare);
				} else {
					fprintf(stderr, "%s:%d: unknown sort method: '%s'\n",
					        zScriptFile, sScript.startLine, sScript.azToken[2]);
					REQUIRE(false);
				}

				/* Hash the results if we are over the hash threshold or if we
				** there is a hash label */
				if (sScript.azToken[3][0] ||
				    (hashThreshold > 0 && nResult > hashThreshold)) {
					md5_add(
					    ""); /* make sure md5 is reset, even if no results */
					for (i = 0; i < nResult; i++) {
						md5_add(azResult[i]);
						md5_add("\n");
					}
					snprintf(zHash, sizeof(zHash), "%d values hashing to %s",
					         nResult, md5_finish());
					sScript.azToken[3][20] = 0;
					if (sScript.azToken[3][0] &&
					    checkValue(sScript.azToken[3], md5_finish())) {
						fprintf(
						    stderr,
						    "%s:%d: labeled result [%s] does not agree with "
						    "previous values\n",
						    zScriptFile, sScript.startLine, sScript.azToken[3]);
						REQUIRE(false);
					}
				}

				if (verifyMode) {
					/* In verify mode, first skip over the ---- line if we are
					 *still
					 ** pointing at it. */
					if (strcmp(sScript.zLine, "----") == 0)
						nextLine(&sScript);

					/* Compare subsequent lines of the script against the
					 *results
					 ** from the query.  Report an error if any differences are
					 *found.
					 */
					if (hashThreshold == 0 || nResult <= hashThreshold) {
						for (i = 0; i < nResult && sScript.zLine[0];
						     nextLine(&sScript), i++) {
							if (strcmp(sScript.zLine, azResult[i]) != 0) {
								fprintf(stdout, "%s:%d: wrong result\n",
								        zScriptFile, sScript.nLine);

								fprintf(stdout, "%s <> %s\n", sScript.zLine,
								        azResult[i]);
								REQUIRE(false);
								break;
							}
							// we check this already but this inflates the test
							// case count as desired
							REQUIRE(strcmp(sScript.zLine, azResult[i]) == 0);
						}
					} else {
						if (strcmp(sScript.zLine, zHash) != 0) {
							fprintf(stderr, "%s:%d: wrong result hash\n",
							        zScriptFile, sScript.nLine);
							REQUIRE(false);
						}
					}
				} else {
					/* In completion mode, first make sure we have output an
					 *---- line.
					 ** Output such a line now if we have not already done so.
					 */
					if (strcmp(sScript.zLine, "----") != 0) {
						printf("----\n");
					}

					/* Output the results obtained by running the query
					 */
					if (hashThreshold == 0 || nResult <= hashThreshold) {
						for (i = 0; i < nResult; i++) {
							printf("%s\n", azResult[i]);
						}
					} else {
						printf("%s\n", zHash);
					}
					printf("\n");

					/* Skip over any existing results.  They will be ignored.
					 */
					sScript.copyFlag = 0;
					while (sScript.zLine[0] != 0 &&
					       sScript.iCur < sScript.iEnd) {
						nextLine(&sScript);
					}
					sScript.copyFlag = 1;
				}

				/* Free the query results */
				pEngine->xFreeResults(pConn, azResult, nResult);
			} else if (strcmp(sScript.azToken[0], "hash-threshold") == 0) {
				/* Set the maximum number of result values that will be accepted
				** for a query.  If the number of result values exceeds this
				*number,
				** then an MD5 hash is computed of all values, and the resulting
				*hash
				** is the only result.
				**
				** If the threshold is 0, then hashing is never used.
				**
				** If a threshold was specified on the command line, ignore
				** any specifed in the script.
				*/
				if (!bHt) {
					hashThreshold = atoi(sScript.azToken[1]);
				}
			} else if (strcmp(sScript.azToken[0], "halt") == 0) {
				/* Used for debugging.  Stop reading the test script and shut
				 *down.
				 ** A "halt" record can be inserted in the middle of a test
				 *script in
				 ** to run the script up to a particular point that is giving a
				 ** faulty result, then terminate at that point for analysis.
				 */
				fprintf(stdout, "%s:%d: halt\n", zScriptFile,
				        sScript.startLine);
				break;
			} else {
				/* An unrecognized record type is an error */
				fprintf(stderr, "%s:%d: unknown record type: '%s'\n",
				        zScriptFile, sScript.startLine, sScript.azToken[0]);
				REQUIRE(false);
				break;
			}
		}

		/* Shutdown the database connection.
		 */
		rc = pEngine->xDisconnect(pConn);
		REQUIRE(rc == 0);
		free(zScript);
	}
}
