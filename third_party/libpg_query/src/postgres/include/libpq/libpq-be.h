/*-------------------------------------------------------------------------
 *
 * libpq_be.h
 *	  This file contains definitions for structures and externs used
 *	  by the postmaster during client authentication.
 *
 *	  Note that this is backend-internal and is NOT exported to clients.
 *	  Structs that need to be client-visible are in pqcomm.h.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/libpq/libpq-be.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LIBPQ_BE_H
#define LIBPQ_BE_H

#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif
#ifdef USE_OPENSSL
#include <openssl/ssl.h>
#include <openssl/err.h>
#endif
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif

#ifdef ENABLE_GSS
#if defined(HAVE_GSSAPI_H)
#include <gssapi.h>
#else
#include <gssapi/gssapi.h>
#endif   /* HAVE_GSSAPI_H */
/*
 * GSSAPI brings in headers that set a lot of things in the global namespace on win32,
 * that doesn't match the msvc build. It gives a bunch of compiler warnings that we ignore,
 * but also defines a symbol that simply does not exist. Undefine it again.
 */
#ifdef WIN32_ONLY_COMPILER
#undef HAVE_GETADDRINFO
#endif
#endif   /* ENABLE_GSS */

#ifdef ENABLE_SSPI
#define SECURITY_WIN32
#if defined(WIN32) && !defined(WIN32_ONLY_COMPILER)
#include <ntsecapi.h>
#endif
#include <security.h>
#undef SECURITY_WIN32

#ifndef ENABLE_GSS
/*
 * Define a fake structure compatible with GSSAPI on Unix.
 */
typedef struct
{
	void	   *value;
	int			length;
} gss_buffer_desc;
#endif
#endif   /* ENABLE_SSPI */

#include "datatype/timestamp.h"
#include "libpq/hba.h"
#include "libpq/pqcomm.h"


typedef enum CAC_state
{
	CAC_OK, CAC_STARTUP, CAC_SHUTDOWN, CAC_RECOVERY, CAC_TOOMANY,
	CAC_WAITBACKUP
} CAC_state;


/*
 * GSSAPI specific state information
 */
#if defined(ENABLE_GSS) | defined(ENABLE_SSPI)
typedef struct
{
	gss_buffer_desc outbuf;		/* GSSAPI output token buffer */
#ifdef ENABLE_GSS
	gss_cred_id_t cred;			/* GSSAPI connection cred's */
	gss_ctx_id_t ctx;			/* GSSAPI connection context */
	gss_name_t	name;			/* GSSAPI client name */
#endif
} pg_gssinfo;
#endif

/*
 * This is used by the postmaster in its communication with frontends.  It
 * contains all state information needed during this communication before the
 * backend is run.  The Port structure is kept in malloc'd memory and is
 * still available when a backend is running (see MyProcPort).  The data
 * it points to must also be malloc'd, or else palloc'd in TopMemoryContext,
 * so that it survives into PostgresMain execution!
 *
 * remote_hostname is set if we did a successful reverse lookup of the
 * client's IP address during connection setup.
 * remote_hostname_resolv tracks the state of hostname verification:
 *	+1 = remote_hostname is known to resolve to client's IP address
 *	-1 = remote_hostname is known NOT to resolve to client's IP address
 *	 0 = we have not done the forward DNS lookup yet
 *	-2 = there was an error in name resolution
 * If reverse lookup of the client IP address fails, remote_hostname will be
 * left NULL while remote_hostname_resolv is set to -2.  If reverse lookup
 * succeeds but forward lookup fails, remote_hostname_resolv is also set to -2
 * (the case is distinguishable because remote_hostname isn't NULL).  In
 * either of the -2 cases, remote_hostname_errcode saves the lookup return
 * code for possible later use with gai_strerror.
 */

typedef struct Port
{
	pgsocket	sock;			/* File descriptor */
	bool		noblock;		/* is the socket in non-blocking mode? */
	ProtocolVersion proto;		/* FE/BE protocol version */
	SockAddr	laddr;			/* local addr (postmaster) */
	SockAddr	raddr;			/* remote addr (client) */
	char	   *remote_host;	/* name (or ip addr) of remote host */
	char	   *remote_hostname;/* name (not ip addr) of remote host, if
								 * available */
	int			remote_hostname_resolv; /* see above */
	int			remote_hostname_errcode;		/* see above */
	char	   *remote_port;	/* text rep of remote port */
	CAC_state	canAcceptConnections;	/* postmaster connection status */

	/*
	 * Information that needs to be saved from the startup packet and passed
	 * into backend execution.  "char *" fields are NULL if not set.
	 * guc_options points to a List of alternating option names and values.
	 */
	char	   *database_name;
	char	   *user_name;
	char	   *cmdline_options;
	List	   *guc_options;

	/*
	 * Information that needs to be held during the authentication cycle.
	 */
	HbaLine    *hba;
	char		md5Salt[4];		/* Password salt */

	/*
	 * Information that really has no business at all being in struct Port,
	 * but since it gets used by elog.c in the same way as database_name and
	 * other members of this struct, we may as well keep it here.
	 */
	TimestampTz SessionStartTime;		/* backend start time */

	/*
	 * TCP keepalive settings.
	 *
	 * default values are 0 if AF_UNIX or not yet known; current values are 0
	 * if AF_UNIX or using the default. Also, -1 in a default value means we
	 * were unable to find out the default (getsockopt failed).
	 */
	int			default_keepalives_idle;
	int			default_keepalives_interval;
	int			default_keepalives_count;
	int			keepalives_idle;
	int			keepalives_interval;
	int			keepalives_count;

#if defined(ENABLE_GSS) || defined(ENABLE_SSPI)

	/*
	 * If GSSAPI is supported, store GSSAPI information. Otherwise, store a
	 * NULL pointer to make sure offsets in the struct remain the same.
	 */
	pg_gssinfo *gss;
#else
	void	   *gss;
#endif

	/*
	 * SSL structures.
	 */
	bool		ssl_in_use;
	char	   *peer_cn;
	bool		peer_cert_valid;

	/*
	 * OpenSSL structures. (Keep these last so that the locations of other
	 * fields are the same whether or not you build with OpenSSL.)
	 */
#ifdef USE_OPENSSL
	SSL		   *ssl;
	X509	   *peer;
	unsigned long count;
#endif
} Port;

#ifdef USE_SSL
/*
 * These functions are implemented by the glue code specific to each
 * SSL implementation (e.g. be-secure-openssl.c)
 */
extern void be_tls_init(void);
extern int	be_tls_open_server(Port *port);
extern void be_tls_close(Port *port);
extern ssize_t be_tls_read(Port *port, void *ptr, size_t len, int *waitfor);
extern ssize_t be_tls_write(Port *port, void *ptr, size_t len, int *waitfor);

extern int	be_tls_get_cipher_bits(Port *port);
extern bool be_tls_get_compression(Port *port);
extern void be_tls_get_version(Port *port, char *ptr, size_t len);
extern void be_tls_get_cipher(Port *port, char *ptr, size_t len);
extern void be_tls_get_peerdn_name(Port *port, char *ptr, size_t len);
#endif

extern ProtocolVersion FrontendProtocol;

/* TCP keepalives configuration. These are no-ops on an AF_UNIX socket. */

extern int	pq_getkeepalivesidle(Port *port);
extern int	pq_getkeepalivesinterval(Port *port);
extern int	pq_getkeepalivescount(Port *port);

extern int	pq_setkeepalivesidle(int idle, Port *port);
extern int	pq_setkeepalivesinterval(int interval, Port *port);
extern int	pq_setkeepalivescount(int count, Port *port);

#endif   /* LIBPQ_BE_H */
