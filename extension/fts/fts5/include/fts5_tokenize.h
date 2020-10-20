// Make SQLite's Porter stemmer available to DuckDB
#ifndef FTS_TOKENIZE_H
#define FTS_TOKENIZE_H

#ifdef __cplusplus
extern "C" {
#endif

#define FTS5_PORTER_MAX_TOKEN

typedef struct PorterContext {
  void *pCtx;
  int (*xToken)(void*, int, const char*, int, int, int);
  char *aBuf;
} PorterContext;

int fts5PorterCb(void *pCtx, int tflags, const char *pToken, int nToken, int iStart, int iEnd);

#ifdef __cplusplus
};
#endif

#endif