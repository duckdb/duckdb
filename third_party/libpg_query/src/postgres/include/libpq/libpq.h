/*-------------------------------------------------------------------------
 *
 * libpq.h
 *	  POSTGRES LIBPQ buffer structure definitions.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/libpq/libpq.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LIBPQ_H
#define LIBPQ_H

#include <sys/types.h>
#include <netinet/in.h>

#include "lib/stringinfo.h"
#include "libpq/libpq-be.h"


typedef struct
{
	void		(*comm_reset) (void);
	int			(*flush) (void);
	int			(*flush_if_writable) (void);
	bool		(*is_send_pending) (void);
	int			(*putmessage) (char msgtype, const char *s, size_t len);
	void		(*putmessage_noblock) (char msgtype, const char *s, size_t len);
	void		(*startcopyout) (void);
	void		(*endcopyout) (bool errorAbort);
} PQcommMethods;

extern PGDLLIMPORT PQcommMethods *PqCommMethods;

#define pq_comm_reset() (PqCommMethods->comm_reset())
#define pq_flush() (PqCommMethods->flush())
#define pq_flush_if_writable() (PqCommMethods->flush_if_writable())
#define pq_is_send_pending() (PqCommMethods->is_send_pending())
#define pq_putmessage(msgtype, s, len) \
	(PqCommMethods->putmessage(msgtype, s, len))
#define pq_putmessage_noblock(msgtype, s, len) \
	(PqCommMethods->putmessage(msgtype, s, len))
#define pq_startcopyout() (PqCommMethods->startcopyout())
#define pq_endcopyout(errorAbort) (PqCommMethods->endcopyout(errorAbort))

/*
 * External functions.
 */

/*
 * prototypes for functions in pqcomm.c
 */
extern int StreamServerPort(int family, char *hostName,
				 unsigned short portNumber, char *unixSocketDir,
				 pgsocket ListenSocket[], int MaxListen);
extern int	StreamConnection(pgsocket server_fd, Port *port);
extern void StreamClose(pgsocket sock);
extern void TouchSocketFiles(void);
extern void RemoveSocketFiles(void);
extern void pq_init(void);
extern int	pq_getbytes(char *s, size_t len);
extern int	pq_getstring(StringInfo s);
extern void pq_startmsgread(void);
extern void pq_endmsgread(void);
extern bool pq_is_reading_msg(void);
extern int	pq_getmessage(StringInfo s, int maxlen);
extern int	pq_getbyte(void);
extern int	pq_peekbyte(void);
extern int	pq_getbyte_if_available(unsigned char *c);
extern int	pq_putbytes(const char *s, size_t len);

/*
 * prototypes for functions in be-secure.c
 */
extern char *ssl_cert_file;
extern char *ssl_key_file;
extern char *ssl_ca_file;
extern char *ssl_crl_file;

extern int	(*pq_putmessage_hook) (char msgtype, const char *s, size_t len);
extern int	(*pq_flush_hook) (void);

extern int	secure_initialize(void);
extern bool secure_loaded_verify_locations(void);
extern void secure_destroy(void);
extern int	secure_open_server(Port *port);
extern void secure_close(Port *port);
extern ssize_t secure_read(Port *port, void *ptr, size_t len);
extern ssize_t secure_write(Port *port, void *ptr, size_t len);
extern ssize_t secure_raw_read(Port *port, void *ptr, size_t len);
extern ssize_t secure_raw_write(Port *port, const void *ptr, size_t len);

extern bool ssl_loaded_verify_locations;

/* GUCs */
extern char *SSLCipherSuites;
extern char *SSLECDHCurve;
extern bool SSLPreferServerCiphers;

#endif   /* LIBPQ_H */
