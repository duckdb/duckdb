// Make SQLite's Porter stemmer available to DuckDB
#ifndef FTS_TOKENIZE_H
#define FTS_TOKENIZE_H

#ifdef __cplusplus
extern "C" {
#endif

/* Any tokens larger than this (in bytes) are passed through without
** stemming. */
#define FTS5_PORTER_MAX_TOKEN   64

int fts5PorterCb(char *aBuf, const char *pToken, int nToken);

#ifdef __cplusplus
};
#endif

#endif