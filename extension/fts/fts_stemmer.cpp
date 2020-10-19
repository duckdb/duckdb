#include "fts_stemmer.hpp"

#include "fts5_tokenize.h"

namespace duckdb {

string_t Stem(string_t input) {
    void *pCtx = nullptr;
    int flags = 0x0004;
    const char *pText = input.GetData();
    int nText = input.GetSize();

    char aBuf[FTS5_PORTER_MAX_TOKEN + 64];

    PorterContext sCtx;
    sCtx.xToken = fts5PorterCb;
    sCtx.pCtx = pCtx;
    sCtx.aBuf = aBuf;
    
    int nBuf = fts5PorterCb((void*)&sCtx, flags, pText, nText, 0, nText - 1);

    return string_t(aBuf, nBuf);
}

} // namespace duckdb
