// Make SQLite's Porter stemmer available to DuckDB

#ifdef __cplusplus
extern "C" {
#endif

#define FTS5_PORTER_MAX_TOKEN

typedef struct PorterContext PorterContext;
struct PorterContext {
  void *pCtx;
  int (*xToken)(void*, int, const char*, int, int, int);
  char *aBuf;
};

int fts5PorterCb(
  void *pCtx, 
  int tflags,
  const char *pToken, 
  int nToken, 
  int iStart, 
  int iEnd
);

#ifdef __cplusplus
}
#endif