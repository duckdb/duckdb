/*-------------------------------------------------------------------------
 *
 * pqcomm.h
 *		Definitions common to frontends and backends.
 *
 * NOTE: for historical reasons, this does not correspond to pqcomm.c.
 * pqcomm.c's routines are declared in libpq.h.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/libpq/pqcomm.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PQCOMM_H
#define PQCOMM_H

#include <sys/socket.h>
#include <netdb.h>
#ifdef HAVE_SYS_UN_H
#include <sys/un.h>
#endif
#include <netinet/in.h>

#ifdef HAVE_STRUCT_SOCKADDR_STORAGE

#ifndef HAVE_STRUCT_SOCKADDR_STORAGE_SS_FAMILY
#ifdef HAVE_STRUCT_SOCKADDR_STORAGE___SS_FAMILY
#define ss_family __ss_family
#else
#error struct sockaddr_storage does not provide an ss_family member
#endif
#endif

#ifdef HAVE_STRUCT_SOCKADDR_STORAGE___SS_LEN
#define ss_len __ss_len
#define HAVE_STRUCT_SOCKADDR_STORAGE_SS_LEN 1
#endif
#else							/* !HAVE_STRUCT_SOCKADDR_STORAGE */

/* Define a struct sockaddr_storage if we don't have one. */

struct sockaddr_storage
{
	union
	{
		struct sockaddr sa;		/* get the system-dependent fields */
		int64		ss_align;	/* ensures struct is properly aligned */
		char		ss_pad[128];	/* ensures struct has desired size */
	}			ss_stuff;
};

#define ss_family	ss_stuff.sa.sa_family
/* It should have an ss_len field if sockaddr has sa_len. */
#ifdef HAVE_STRUCT_SOCKADDR_SA_LEN
#define ss_len		ss_stuff.sa.sa_len
#define HAVE_STRUCT_SOCKADDR_STORAGE_SS_LEN 1
#endif
#endif   /* HAVE_STRUCT_SOCKADDR_STORAGE */

typedef struct
{
	struct sockaddr_storage addr;
	ACCEPT_TYPE_ARG3 salen;
} SockAddr;

/* Configure the UNIX socket location for the well known port. */

#define UNIXSOCK_PATH(path, port, sockdir) \
		snprintf(path, sizeof(path), "%s/.s.PGSQL.%d", \
				((sockdir) && *(sockdir) != '\0') ? (sockdir) : \
				DEFAULT_PGSOCKET_DIR, \
				(port))

/*
 * The maximum workable length of a socket path is what will fit into
 * struct sockaddr_un.  This is usually only 100 or so bytes :-(.
 *
 * For consistency, always pass a MAXPGPATH-sized buffer to UNIXSOCK_PATH(),
 * then complain if the resulting string is >= UNIXSOCK_PATH_BUFLEN bytes.
 * (Because the standard API for getaddrinfo doesn't allow it to complain in
 * a useful way when the socket pathname is too long, we have to test for
 * this explicitly, instead of just letting the subroutine return an error.)
 */
#define UNIXSOCK_PATH_BUFLEN sizeof(((struct sockaddr_un *) NULL)->sun_path)


/*
 * These manipulate the frontend/backend protocol version number.
 *
 * The major number should be incremented for incompatible changes.  The minor
 * number should be incremented for compatible changes (eg. additional
 * functionality).
 *
 * If a backend supports version m.n of the protocol it must actually support
 * versions m.[0..n].  Backend support for version m-1 can be dropped after a
 * `reasonable' length of time.
 *
 * A frontend isn't required to support anything other than the current
 * version.
 */

#define PG_PROTOCOL_MAJOR(v)	((v) >> 16)
#define PG_PROTOCOL_MINOR(v)	((v) & 0x0000ffff)
#define PG_PROTOCOL(m,n)	(((m) << 16) | (n))

/* The earliest and latest frontend/backend protocol version supported. */

#define PG_PROTOCOL_EARLIEST	PG_PROTOCOL(1,0)
#define PG_PROTOCOL_LATEST		PG_PROTOCOL(3,0)

typedef uint32 ProtocolVersion; /* FE/BE protocol version number */

typedef ProtocolVersion MsgType;


/*
 * Packet lengths are 4 bytes in network byte order.
 *
 * The initial length is omitted from the packet layouts appearing below.
 */

typedef uint32 PacketLen;


/*
 * Old-style startup packet layout with fixed-width fields.  This is used in
 * protocol 1.0 and 2.0, but not in later versions.  Note that the fields
 * in this layout are '\0' terminated only if there is room.
 */

#define SM_DATABASE		64
#define SM_USER			32
/* We append database name if db_user_namespace true. */
#define SM_DATABASE_USER (SM_DATABASE+SM_USER+1)		/* +1 for @ */
#define SM_OPTIONS		64
#define SM_UNUSED		64
#define SM_TTY			64

typedef struct StartupPacket
{
	ProtocolVersion protoVersion;		/* Protocol version */
	char		database[SM_DATABASE];	/* Database name */
	/* Db_user_namespace appends dbname */
	char		user[SM_USER];	/* User name */
	char		options[SM_OPTIONS];	/* Optional additional args */
	char		unused[SM_UNUSED];		/* Unused */
	char		tty[SM_TTY];	/* Tty for debug output */
} StartupPacket;

extern bool Db_user_namespace;

/*
 * In protocol 3.0 and later, the startup packet length is not fixed, but
 * we set an arbitrary limit on it anyway.  This is just to prevent simple
 * denial-of-service attacks via sending enough data to run the server
 * out of memory.
 */
#define MAX_STARTUP_PACKET_LENGTH 10000


/* These are the authentication request codes sent by the backend. */

#define AUTH_REQ_OK			0	/* User is authenticated  */
#define AUTH_REQ_KRB4		1	/* Kerberos V4. Not supported any more. */
#define AUTH_REQ_KRB5		2	/* Kerberos V5. Not supported any more. */
#define AUTH_REQ_PASSWORD	3	/* Password */
#define AUTH_REQ_CRYPT		4	/* crypt password. Not supported any more. */
#define AUTH_REQ_MD5		5	/* md5 password */
#define AUTH_REQ_SCM_CREDS	6	/* transfer SCM credentials */
#define AUTH_REQ_GSS		7	/* GSSAPI without wrap() */
#define AUTH_REQ_GSS_CONT	8	/* Continue GSS exchanges */
#define AUTH_REQ_SSPI		9	/* SSPI negotiate without wrap() */

typedef uint32 AuthRequest;


/*
 * A client can also send a cancel-current-operation request to the postmaster.
 * This is uglier than sending it directly to the client's backend, but it
 * avoids depending on out-of-band communication facilities.
 *
 * The cancel request code must not match any protocol version number
 * we're ever likely to use.  This random choice should do.
 */
#define CANCEL_REQUEST_CODE PG_PROTOCOL(1234,5678)

typedef struct CancelRequestPacket
{
	/* Note that each field is stored in network byte order! */
	MsgType		cancelRequestCode;		/* code to identify a cancel request */
	uint32		backendPID;		/* PID of client's backend */
	uint32		cancelAuthCode; /* secret key to authorize cancel */
} CancelRequestPacket;


/*
 * A client can also start by sending a SSL negotiation request, to get a
 * secure channel.
 */
#define NEGOTIATE_SSL_CODE PG_PROTOCOL(1234,5679)

#endif   /* PQCOMM_H */
