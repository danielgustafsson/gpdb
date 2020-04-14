/*-------------------------------------------------------------------------
 *
 * pg_exttable.h
 *	  definitions for system wide external relations
 *
 * Portions Copyright (c) 2007-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/catalog/pg_exttable.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_EXTTABLE_H
#define PG_EXTTABLE_H

#include "catalog/genbki.h"
#include "nodes/pg_list.h"

/*
 * pg_exttable definition.
 */

/* ----------------
 *		pg_exttable definition.  cpp turns this into
 *		typedef struct FormData_pg_exttable
 * ----------------
 */
#define ExtTableRelationId	6040

CATALOG(pg_exttable,6040) BKI_WITHOUT_OIDS
{
	Oid		reloid;				/* refers to this relation's oid in pg_class  */
#ifdef CATALOG_VARLEN
	text	urilocation[1];		/* array of URI strings */
	text	execlocation[1];	/* array of ON locations */
	char	fmttype;			/* 't' (text) or 'c' (csv) */
	text	command;			/* the command string to EXECUTE */
	int32	rejectlimit;		/* error count reject limit per segment */
	char	rejectlimittype;	/* 'r' (rows) or 'p' (percent) */
	char	logerrors;			/* 't' to log errors into file, 'f' to disable log error, 'p' means log errors persistently */
	int32	encoding;			/* character encoding of this external table */
	bool	writable;			/* 't' if writable, 'f' if readable */
#endif
} FormData_pg_exttable;

/* GPDB added foreign key definitions for gpcheckcat. */
FOREIGN_KEY(reloid REFERENCES pg_class(oid));

/* ----------------
 *		Form_pg_exttable corresponds to a pointer to a tuple with
 *		the format of pg_exttable relation.
 * ----------------
 */
typedef FormData_pg_exttable *Form_pg_exttable;


/* ----------------
 *		compiler constants for pg_exttable
 * ----------------
 */
#define Natts_pg_exttable					10
#define Anum_pg_exttable_reloid				1
#define Anum_pg_exttable_urilocation			2
#define Anum_pg_exttable_execlocation			3
#define Anum_pg_exttable_fmttype			4
#define Anum_pg_exttable_command			5
#define Anum_pg_exttable_rejectlimit		6
#define Anum_pg_exttable_rejectlimittype	7
#define Anum_pg_exttable_logerrors			8
#define Anum_pg_exttable_encoding			9
#define Anum_pg_exttable_writable			10


/*
 * Descriptor of a single AO relation.
 * For now very similar to the catalog row itself but may change in time.
 */
typedef struct ExtTableEntry
{
	List*	urilocations;
	List*	execlocations;
	char	fmtcode;
	List*	options;
	char*	command;
	int		rejectlimit;
	char	rejectlimittype;
	char	logerrors;
    int		encoding;
    bool	iswritable;
    bool	isweb;		/* extra state, not cataloged */
} ExtTableEntry;

/* No initial contents. */

extern void ValidateExtTableOptions(List *options);

extern bool ExtractErrorLogPersistent(List *options);

extern void InsertExtTableEntry(Oid 	tbloid,
					bool 	iswritable,
					bool	issreh,
					char	formattype,
					char	rejectlimittype,
					char*	commandString,
					int		rejectlimit,
					char	logerrors,
					int		encoding,
					Datum	locationExec,
					Datum	locationUris);

extern ExtTableEntry *GetExtTableEntry(Oid relid);
extern ExtTableEntry *GetExtTableEntryIfExists(Oid relid);

extern void RemoveExtTableEntry(Oid relid);

#define fmttype_is_custom(c) (c == 'b')
#define fmttype_is_text(c)   (c == 't')
#define fmttype_is_csv(c)    (c == 'c')

#endif /* PG_EXTTABLE_H */
