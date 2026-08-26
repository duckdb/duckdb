/*
 *  FIPS-180-1 compliant SHA-1 implementation
 *
 *  Copyright The Mbed TLS Contributors
 *  SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
 */
/*
 *  The SHA-1 standard was published by NIST in 1993.
 *
 *  http://www.itl.nist.gov/fipspubs/fip180-1.htm
 */

#include "common.h"

#if defined(MBEDTLS_SHA1_C)

#include "mbedtls/sha1.h"
#include "mbedtls/platform_util.h"
#include "mbedtls/error.h"

#include <string.h>

#include "mbedtls/platform.h"

#if !defined(MBEDTLS_SHA1_ALT)

void mbedtls_sha1_init(mbedtls_sha1_context *ctx)
{
    memset(ctx, 0, sizeof(mbedtls_sha1_context));
}

void mbedtls_sha1_free(mbedtls_sha1_context *ctx)
{
    if (ctx == NULL) {
        return;
    }

    mbedtls_platform_zeroize(ctx, sizeof(mbedtls_sha1_context));
}

void mbedtls_sha1_clone(mbedtls_sha1_context *dst,
                        const mbedtls_sha1_context *src)
{
    *dst = *src;
}

/*
 * SHA-1 context setup
 */
int mbedtls_sha1_starts(mbedtls_sha1_context *ctx)
{
    ctx->total[0] = 0;
    ctx->total[1] = 0;

    ctx->state[0] = 0x67452301;
    ctx->state[1] = 0xEFCDAB89;
    ctx->state[2] = 0x98BADCFE;
    ctx->state[3] = 0x10325476;
    ctx->state[4] = 0xC3D2E1F0;

    return 0;
}

#if !defined(MBEDTLS_SHA1_PROCESS_ALT)
int mbedtls_internal_sha1_process(mbedtls_sha1_context *ctx,
                                  const unsigned char data[64])
{
    struct {
        uint32_t temp, W[16], A, B, C, D, E;
    } local;

    local.W[0] = MBEDTLS_GET_UINT32_BE(data,  0);
    local.W[1] = MBEDTLS_GET_UINT32_BE(data,  4);
    local.W[2] = MBEDTLS_GET_UINT32_BE(data,  8);
    local.W[3] = MBEDTLS_GET_UINT32_BE(data, 12);
    local.W[4] = MBEDTLS_GET_UINT32_BE(data, 16);
    local.W[5] = MBEDTLS_GET_UINT32_BE(data, 20);
    local.W[6] = MBEDTLS_GET_UINT32_BE(data, 24);
    local.W[7] = MBEDTLS_GET_UINT32_BE(data, 28);
    local.W[8] = MBEDTLS_GET_UINT32_BE(data, 32);
    local.W[9] = MBEDTLS_GET_UINT32_BE(data, 36);
    local.W[10] = MBEDTLS_GET_UINT32_BE(data, 40);
    local.W[11] = MBEDTLS_GET_UINT32_BE(data, 44);
    local.W[12] = MBEDTLS_GET_UINT32_BE(data, 48);
    local.W[13] = MBEDTLS_GET_UINT32_BE(data, 52);
    local.W[14] = MBEDTLS_GET_UINT32_BE(data, 56);
    local.W[15] = MBEDTLS_GET_UINT32_BE(data, 60);

#define S(x, n) (((x) << (n)) | (((x) & 0xFFFFFFFF) >> (32 - (n))))

#define SHA1R(t)                                                    \
    (                                                           \
        local.temp = local.W[((t) -  3) & 0x0F] ^             \
                     local.W[((t) -  8) & 0x0F] ^             \
                     local.W[((t) - 14) & 0x0F] ^             \
                     local.W[(t)        & 0x0F],              \
        (local.W[(t) & 0x0F] = S(local.temp, 1))               \
    )

#define SHA1P(a, b, c, d, e, x)                                          \
    do                                                          \
    {                                                           \
        (e) += S((a), 5) + F((b), (c), (d)) + K + (x);             \
        (b) = S((b), 30);                                        \
    } while (0)

    local.A = ctx->state[0];
    local.B = ctx->state[1];
    local.C = ctx->state[2];
    local.D = ctx->state[3];
    local.E = ctx->state[4];

#define F(x, y, z) ((z) ^ ((x) & ((y) ^ (z))))
#define K 0x5A827999

    SHA1P(local.A, local.B, local.C, local.D, local.E, local.W[0]);
    SHA1P(local.E, local.A, local.B, local.C, local.D, local.W[1]);
    SHA1P(local.D, local.E, local.A, local.B, local.C, local.W[2]);
    SHA1P(local.C, local.D, local.E, local.A, local.B, local.W[3]);
    SHA1P(local.B, local.C, local.D, local.E, local.A, local.W[4]);
    SHA1P(local.A, local.B, local.C, local.D, local.E, local.W[5]);
    SHA1P(local.E, local.A, local.B, local.C, local.D, local.W[6]);
    SHA1P(local.D, local.E, local.A, local.B, local.C, local.W[7]);
    SHA1P(local.C, local.D, local.E, local.A, local.B, local.W[8]);
    SHA1P(local.B, local.C, local.D, local.E, local.A, local.W[9]);
    SHA1P(local.A, local.B, local.C, local.D, local.E, local.W[10]);
    SHA1P(local.E, local.A, local.B, local.C, local.D, local.W[11]);
    SHA1P(local.D, local.E, local.A, local.B, local.C, local.W[12]);
    SHA1P(local.C, local.D, local.E, local.A, local.B, local.W[13]);
    SHA1P(local.B, local.C, local.D, local.E, local.A, local.W[14]);
    SHA1P(local.A, local.B, local.C, local.D, local.E, local.W[15]);
    SHA1P(local.E, local.A, local.B, local.C, local.D, SHA1R(16));
    SHA1P(local.D, local.E, local.A, local.B, local.C, SHA1R(17));
    SHA1P(local.C, local.D, local.E, local.A, local.B, SHA1R(18));
    SHA1P(local.B, local.C, local.D, local.E, local.A, SHA1R(19));

#undef K
#undef F

#define F(x, y, z) ((x) ^ (y) ^ (z))
#define K 0x6ED9EBA1

    SHA1P(local.A, local.B, local.C, local.D, local.E, SHA1R(20));
    SHA1P(local.E, local.A, local.B, local.C, local.D, SHA1R(21));
    SHA1P(local.D, local.E, local.A, local.B, local.C, SHA1R(22));
    SHA1P(local.C, local.D, local.E, local.A, local.B, SHA1R(23));
    SHA1P(local.B, local.C, local.D, local.E, local.A, SHA1R(24));
    SHA1P(local.A, local.B, local.C, local.D, local.E, SHA1R(25));
    SHA1P(local.E, local.A, local.B, local.C, local.D, SHA1R(26));
    SHA1P(local.D, local.E, local.A, local.B, local.C, SHA1R(27));
    SHA1P(local.C, local.D, local.E, local.A, local.B, SHA1R(28));
    SHA1P(local.B, local.C, local.D, local.E, local.A, SHA1R(29));
    SHA1P(local.A, local.B, local.C, local.D, local.E, SHA1R(30));
    SHA1P(local.E, local.A, local.B, local.C, local.D, SHA1R(31));
    SHA1P(local.D, local.E, local.A, local.B, local.C, SHA1R(32));
    SHA1P(local.C, local.D, local.E, local.A, local.B, SHA1R(33));
    SHA1P(local.B, local.C, local.D, local.E, local.A, SHA1R(34));
    SHA1P(local.A, local.B, local.C, local.D, local.E, SHA1R(35));
    SHA1P(local.E, local.A, local.B, local.C, local.D, SHA1R(36));
    SHA1P(local.D, local.E, local.A, local.B, local.C, SHA1R(37));
    SHA1P(local.C, local.D, local.E, local.A, local.B, SHA1R(38));
    SHA1P(local.B, local.C, local.D, local.E, local.A, SHA1R(39));

#undef K
#undef F

#define F(x, y, z) (((x) & (y)) | ((z) & ((x) | (y))))
#define K 0x8F1BBCDC

    SHA1P(local.A, local.B, local.C, local.D, local.E, SHA1R(40));
    SHA1P(local.E, local.A, local.B, local.C, local.D, SHA1R(41));
    SHA1P(local.D, local.E, local.A, local.B, local.C, SHA1R(42));
    SHA1P(local.C, local.D, local.E, local.A, local.B, SHA1R(43));
    SHA1P(local.B, local.C, local.D, local.E, local.A, SHA1R(44));
    SHA1P(local.A, local.B, local.C, local.D, local.E, SHA1R(45));
    SHA1P(local.E, local.A, local.B, local.C, local.D, SHA1R(46));
    SHA1P(local.D, local.E, local.A, local.B, local.C, SHA1R(47));
    SHA1P(local.C, local.D, local.E, local.A, local.B, SHA1R(48));
    SHA1P(local.B, local.C, local.D, local.E, local.A, SHA1R(49));
    SHA1P(local.A, local.B, local.C, local.D, local.E, SHA1R(50));
    SHA1P(local.E, local.A, local.B, local.C, local.D, SHA1R(51));
    SHA1P(local.D, local.E, local.A, local.B, local.C, SHA1R(52));
    SHA1P(local.C, local.D, local.E, local.A, local.B, SHA1R(53));
    SHA1P(local.B, local.C, local.D, local.E, local.A, SHA1R(54));
    SHA1P(local.A, local.B, local.C, local.D, local.E, SHA1R(55));
    SHA1P(local.E, local.A, local.B, local.C, local.D, SHA1R(56));
    SHA1P(local.D, local.E, local.A, local.B, local.C, SHA1R(57));
    SHA1P(local.C, local.D, local.E, local.A, local.B, SHA1R(58));
    SHA1P(local.B, local.C, local.D, local.E, local.A, SHA1R(59));

#undef K
#undef F

#define F(x, y, z) ((x) ^ (y) ^ (z))
#define K 0xCA62C1D6

    SHA1P(local.A, local.B, local.C, local.D, local.E, SHA1R(60));
    SHA1P(local.E, local.A, local.B, local.C, local.D, SHA1R(61));
    SHA1P(local.D, local.E, local.A, local.B, local.C, SHA1R(62));
    SHA1P(local.C, local.D, local.E, local.A, local.B, SHA1R(63));
    SHA1P(local.B, local.C, local.D, local.E, local.A, SHA1R(64));
    SHA1P(local.A, local.B, local.C, local.D, local.E, SHA1R(65));
    SHA1P(local.E, local.A, local.B, local.C, local.D, SHA1R(66));
    SHA1P(local.D, local.E, local.A, local.B, local.C, SHA1R(67));
    SHA1P(local.C, local.D, local.E, local.A, local.B, SHA1R(68));
    SHA1P(local.B, local.C, local.D, local.E, local.A, SHA1R(69));
    SHA1P(local.A, local.B, local.C, local.D, local.E, SHA1R(70));
    SHA1P(local.E, local.A, local.B, local.C, local.D, SHA1R(71));
    SHA1P(local.D, local.E, local.A, local.B, local.C, SHA1R(72));
    SHA1P(local.C, local.D, local.E, local.A, local.B, SHA1R(73));
    SHA1P(local.B, local.C, local.D, local.E, local.A, SHA1R(74));
    SHA1P(local.A, local.B, local.C, local.D, local.E, SHA1R(75));
    SHA1P(local.E, local.A, local.B, local.C, local.D, SHA1R(76));
    SHA1P(local.D, local.E, local.A, local.B, local.C, SHA1R(77));
    SHA1P(local.C, local.D, local.E, local.A, local.B, SHA1R(78));
    SHA1P(local.B, local.C, local.D, local.E, local.A, SHA1R(79));

#undef K
#undef F

    ctx->state[0] += local.A;
    ctx->state[1] += local.B;
    ctx->state[2] += local.C;
    ctx->state[3] += local.D;
    ctx->state[4] += local.E;

    /* Zeroise buffers and variables to clear sensitive data from memory. */
    mbedtls_platform_zeroize(&local, sizeof(local));

    return 0;
}

#endif /* !MBEDTLS_SHA1_PROCESS_ALT */

/*
 * SHA-1 process buffer
 */
int mbedtls_sha1_update(mbedtls_sha1_context *ctx,
                        const unsigned char *input,
                        size_t ilen)
{
    int ret = MBEDTLS_ERR_ERROR_CORRUPTION_DETECTED;
    size_t fill;
    uint32_t left;

    if (ilen == 0) {
        return 0;
    }

    left = ctx->total[0] & 0x3F;
    fill = 64 - left;

    ctx->total[0] += (uint32_t) ilen;
    ctx->total[0] &= 0xFFFFFFFF;

    if (ctx->total[0] < (uint32_t) ilen) {
        ctx->total[1]++;
    }

    if (left && ilen >= fill) {
        memcpy((void *) (ctx->buffer + left), input, fill);

        if ((ret = mbedtls_internal_sha1_process(ctx, ctx->buffer)) != 0) {
            return ret;
        }

        input += fill;
        ilen  -= fill;
        left = 0;
    }

    while (ilen >= 64) {
        if ((ret = mbedtls_internal_sha1_process(ctx, input)) != 0) {
            return ret;
        }

        input += 64;
        ilen  -= 64;
    }

    if (ilen > 0) {
        memcpy((void *) (ctx->buffer + left), input, ilen);
    }

    return 0;
}

/*
 * SHA-1 final digest
 */
int mbedtls_sha1_finish(mbedtls_sha1_context *ctx,
                        unsigned char output[20])
{
    int ret = MBEDTLS_ERR_ERROR_CORRUPTION_DETECTED;
    uint32_t used;
    uint32_t high, low;

    /*
     * Add padding: 0x80 then 0x00 until 8 bytes remain for the length
     */
    used = ctx->total[0] & 0x3F;

    ctx->buffer[used++] = 0x80;

    if (used <= 56) {
        /* Enough room for padding + length in current block */
        memset(ctx->buffer + used, 0, 56 - used);
    } else {
        /* We'll need an extra block */
        memset(ctx->buffer + used, 0, 64 - used);

        if ((ret = mbedtls_internal_sha1_process(ctx, ctx->buffer)) != 0) {
            goto exit;
        }

        memset(ctx->buffer, 0, 56);
    }

    /*
     * Add message length
     */
    high = (ctx->total[0] >> 29)
           | (ctx->total[1] <<  3);
    low  = (ctx->total[0] <<  3);

    MBEDTLS_PUT_UINT32_BE(high, ctx->buffer, 56);
    MBEDTLS_PUT_UINT32_BE(low,  ctx->buffer, 60);

    if ((ret = mbedtls_internal_sha1_process(ctx, ctx->buffer)) != 0) {
        goto exit;
    }

    /*
     * Output final state
     */
    MBEDTLS_PUT_UINT32_BE(ctx->state[0], output,  0);
    MBEDTLS_PUT_UINT32_BE(ctx->state[1], output,  4);
    MBEDTLS_PUT_UINT32_BE(ctx->state[2], output,  8);
    MBEDTLS_PUT_UINT32_BE(ctx->state[3], output, 12);
    MBEDTLS_PUT_UINT32_BE(ctx->state[4], output, 16);

    ret = 0;

exit:
    mbedtls_sha1_free(ctx);
    return ret;
}

#endif /* !MBEDTLS_SHA1_ALT */

/*
 * output = SHA-1( input buffer )
 */
int mbedtls_sha1(const unsigned char *input,
                 size_t ilen,
                 unsigned char output[20])
{
    int ret = MBEDTLS_ERR_ERROR_CORRUPTION_DETECTED;
    mbedtls_sha1_context ctx;

    mbedtls_sha1_init(&ctx);

    if ((ret = mbedtls_sha1_starts(&ctx)) != 0) {
        goto exit;
    }

    if ((ret = mbedtls_sha1_update(&ctx, input, ilen)) != 0) {
        goto exit;
    }

    if ((ret = mbedtls_sha1_finish(&ctx, output)) != 0) {
        goto exit;
    }

exit:
    mbedtls_sha1_free(&ctx);
    return ret;
}

#if defined(MBEDTLS_SELF_TEST)
/*
 * FIPS-180-1 test vectors
 */
static const unsigned char sha1_test_buf[3][57] =
{
    { "abc" },
    { "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq" },
    { "" }
};

static const size_t sha1_test_buflen[3] =
{
    3, 56, 1000
};

static const unsigned char sha1_test_sum[3][20] =
{
    { 0xA9, 0x99, 0x3E, 0x36, 0x47, 0x06, 0x81, 0x6A, 0xBA, 0x3E,
      0x25, 0x71, 0x78, 0x50, 0xC2, 0x6C, 0x9C, 0xD0, 0xD8, 0x9D },
    { 0x84, 0x98, 0x3E, 0x44, 0x1C, 0x3B, 0xD2, 0x6E, 0xBA, 0xAE,
      0x4A, 0xA1, 0xF9, 0x51, 0x29, 0xE5, 0xE5, 0x46, 0x70, 0xF1 },
    { 0x34, 0xAA, 0x97, 0x3C, 0xD4, 0xC4, 0xDA, 0xA4, 0xF6, 0x1E,
      0xEB, 0x2B, 0xDB, 0xAD, 0x27, 0x31, 0x65, 0x34, 0x01, 0x6F }
};

/*
 * Checkup routine
 */
int mbedtls_sha1_self_test(int verbose)
{
    int i, j, buflen, ret = 0;
    unsigned char buf[1024];
    unsigned char sha1sum[20];
    mbedtls_sha1_context ctx;

    mbedtls_sha1_init(&ctx);

    /*
     * SHA-1
     */
    for (i = 0; i < 3; i++) {
        if (verbose != 0) {
            mbedtls_printf("  SHA-1 test #%d: ", i + 1);
        }

        if ((ret = mbedtls_sha1_starts(&ctx)) != 0) {
            goto fail;
        }

        if (i == 2) {
            memset(buf, 'a', buflen = 1000);

            for (j = 0; j < 1000; j++) {
                ret = mbedtls_sha1_update(&ctx, buf, buflen);
                if (ret != 0) {
                    goto fail;
                }
            }
        } else {
            ret = mbedtls_sha1_update(&ctx, sha1_test_buf[i],
                                      sha1_test_buflen[i]);
            if (ret != 0) {
                goto fail;
            }
        }

        if ((ret = mbedtls_sha1_finish(&ctx, sha1sum)) != 0) {
            goto fail;
        }

        if (memcmp(sha1sum, sha1_test_sum[i], 20) != 0) {
            ret = 1;
            goto fail;
        }

        if (verbose != 0) {
            mbedtls_printf("passed\n");
        }
    }

    if (verbose != 0) {
        mbedtls_printf("\n");
    }

    goto exit;

fail:
    if (verbose != 0) {
        mbedtls_printf("failed\n");
    }

exit:
    mbedtls_sha1_free(&ctx);

    return ret;
}

#endif /* MBEDTLS_SELF_TEST */

#endif /* MBEDTLS_SHA1_C */
