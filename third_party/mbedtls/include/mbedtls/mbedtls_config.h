// HM: beware of changes here
// there are patches in mbedtls_cipher_base_index & mbedtls_cipher_base_lookup_table
// that break if mbedtls features are changed

#define MBEDTLS_AES_C
#define MBEDTLS_ASN1_PARSE_C
#define MBEDTLS_ASN1_WRITE_C
#define MBEDTLS_BASE64_C
#define MBEDTLS_BIGNUM_C
#define MBEDTLS_CCM_GCM_CAN_AES
#define MBEDTLS_CIPHER_C
#define MBEDTLS_DEPRECATED_REMOVED
#define MBEDTLS_GCM_C
#define MBEDTLS_MD_C
#define MBEDTLS_MD_CAN_SHA256
#define MBEDTLS_MD_LIGHT
#define MBEDTLS_OID_C
#define MBEDTLS_PEM_PARSE_C
#define MBEDTLS_PKCS1_V15
#define MBEDTLS_PK_C
#define MBEDTLS_PK_PARSE_C
#define MBEDTLS_PLATFORM_C
#define MBEDTLS_RSA_C
#define MBEDTLS_SHA1_C
#define MBEDTLS_SHA256_C