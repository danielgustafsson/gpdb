/*-------------------------------------------------------------------------
 *
 * tqual.c
 *	  POSTGRES "time qualification" code, ie, tuple visibility rules.
 *
 * NOTE: all the HeapTupleSatisfies routines will update the tuple's
 * "hint" status bits if we see that the inserting or deleting transaction
 * has now committed or aborted (and it is safe to set the hint bits).
 * If the hint bits are changed, SetBufferCommitInfoNeedsSave is called on
 * the passed-in buffer.  The caller must hold not only a pin, but at least
 * shared buffer content lock on the buffer containing the tuple.
 *
 * NOTE: must check TransactionIdIsInProgress (which looks in PGPROC array)
 * before TransactionIdDidCommit/TransactionIdDidAbort (which look in
 * pg_clog).  Otherwise we have a race condition: we might decide that a
 * just-committed transaction crashed, because none of the tests succeed.
 * xact.c is careful to record commit/abort in pg_clog before it unsets
 * MyProc->xid in PGPROC array.  That fixes that problem, but it also
 * means there is a window where TransactionIdIsInProgress and
 * TransactionIdDidCommit will both return true.  If we check only
 * TransactionIdDidCommit, we could consider a tuple committed when a
 * later GetSnapshotData call will still think the originating transaction
 * is in progress, which leads to application-level inconsistency.	The
 * upshot is that we gotta check TransactionIdIsInProgress first in all
 * code paths, except for a few cases where we are looking at
 * subtransactions of our own main transaction and so there can't be any
 * race condition.
 *
 * Summary of visibility functions:
 *
 *   HeapTupleSatisfiesMVCC()
 *        visible to supplied snapshot, excludes current command
 *   HeapTupleSatisfiesNow()
 *        visible to instant snapshot, excludes current command
 *   HeapTupleSatisfiesUpdate()
 *        like HeapTupleSatisfiesNow(), but with user-supplied command
 *        counter and more complex result
 *   HeapTupleSatisfiesSelf()
 *        visible to instant snapshot and current command
 *   HeapTupleSatisfiesDirty()
 *        like HeapTupleSatisfiesSelf(), but includes open transactions
 *   HeapTupleSatisfiesVacuum()
 *        visible to any running transaction, used by VACUUM
 *   HeapTupleSatisfiesToast()
 *        visible unless part of interrupted vacuum, used for TOAST
 *   HeapTupleSatisfiesAny()
 *        all tuples are visible
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/utils/time/tqual.c,v 1.109.2.1 2008/09/11 14:01:35 alvherre Exp $
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/multixact.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/xact.h"
#include "storage/bufmgr.h"
#include "storage/procarray.h"
#include "utils/tqual.h"

#include "access/twophase.h"  /*max_prepared_xacts*/
#include "miscadmin.h"
#include "lib/stringinfo.h"

#include "utils/guc.h"
#include "utils/memutils.h"


#include "catalog/pg_type.h"
#include "funcapi.h"

#include "utils/builtins.h"

#include "cdb/cdbtm.h"
#include "cdb/cdbvars.h"

#include "access/clog.h"

#include "storage/buffile.h"
#include "access/distributedlog.h"

static SharedSnapshotSlot *SharedSnapshotAdd(int4 slotId);
static SharedSnapshotSlot *SharedSnapshotLookup(int4 slotId);

/* Static variables representing various special snapshot semantics */
SnapshotData SnapshotNowData = {HeapTupleSatisfiesNow};
SnapshotData SnapshotSelfData = {HeapTupleSatisfiesSelf};
SnapshotData SnapshotAnyData = {HeapTupleSatisfiesAny};
SnapshotData SnapshotToastData = {HeapTupleSatisfiesToast};

/*
 * These SnapshotData structs are static to simplify memory allocation
 * (see the hack in GetSnapshotData to avoid repeated malloc/free).
 */
static SnapshotData SerializableSnapshotData = {HeapTupleSatisfiesMVCC};
static SnapshotData LatestSnapshotData = {HeapTupleSatisfiesMVCC};

/* Externally visible pointers to valid snapshots: */
Snapshot	SerializableSnapshot = NULL;
Snapshot	LatestSnapshot = NULL;

/*
 * This pointer is not maintained by this module, but it's convenient
 * to declare it here anyway.  Callers typically assign a copy of
 * GetTransactionSnapshot's result to ActiveSnapshot.
 */
Snapshot	ActiveSnapshot = NULL;

/*
 * These are updated by GetSnapshotData.  We initialize them this way
 * for the convenience of TransactionIdIsInProgress: even in bootstrap
 * mode, we don't want it to say that BootstrapTransactionId is in progress.
 *
 * RecentGlobalXmin is initialized to InvalidTransactionId, to ensure that no
 * one tries to use a stale value.  Readers should ensure that it has been set
 * to something else before using it.
 */
TransactionId TransactionXmin = FirstNormalTransactionId;
TransactionId RecentXmin = FirstNormalTransactionId;
TransactionId RecentGlobalXmin = InvalidTransactionId;


/* local functions */
static bool XidInMVCCSnapshot(TransactionId xid, Snapshot snapshot, bool isXmax,
			  bool distributedSnapshotIgnore, bool *setDistributedSnapshotIgnore);
static bool XidInMVCCSnapshot_Local(TransactionId xid, Snapshot snapshot, bool isXmax);

/* MPP Addition. Distributed Snapshot that gets sent in from the QD to processes
 * running in EXECUTE mode.
 */
DtxContext DistributedTransactionContext = DTX_CONTEXT_LOCAL_ONLY;

DtxContextInfo QEDtxContextInfo = DtxContextInfo_StaticInit;

/* MPP Shared Snapshot. */
typedef struct SharedSnapshotStruct
{
	int 		numSlots;		/* number of valid Snapshot entries */
	int			maxSlots;		/* allocated size of sharedSnapshotArray */
	int 		nextSlot;		/* points to the next avail slot. */

	/*
	 * We now allow direct indexing into this array.
	 *
	 * We allocate the XIPS below.
	 *
	 * Be very careful when accessing fields inside here.
	 */
	SharedSnapshotSlot	   *slots;

	TransactionId	   *xips;		/* VARIABLE LENGTH ARRAY */
} SharedSnapshotStruct;

static volatile SharedSnapshotStruct *sharedSnapshotArray;

volatile SharedSnapshotSlot *SharedLocalSnapshotSlot = NULL;

static Size slotSize = 0;
static Size slotCount = 0;
static Size xipEntryCount = 0;

/*
 * Report shared-memory space needed by CreateSharedSnapshot.
 */
Size
SharedSnapshotShmemSize(void)
{
	Size		size;

	xipEntryCount = MaxBackends + max_prepared_xacts;

	slotSize = sizeof(SharedSnapshotSlot);
	slotSize += mul_size(sizeof(TransactionId), (xipEntryCount));
	slotSize = MAXALIGN(slotSize);

	/*
	 * We only really need max_prepared_xacts; but for safety we
	 * multiply that by two (to account for slow de-allocation on
	 * cleanup, for instance).
	 */
	slotCount = 2 * max_prepared_xacts;

	size = offsetof(SharedSnapshotStruct, xips);
	size = add_size(size, mul_size(slotSize, slotCount));

	return MAXALIGN(size);
}

/*
 * Initialize the sharedSnapshot array.  This array is used to communicate
 * snapshots between qExecs that are segmates.
 */
void
CreateSharedSnapshotArray(void)
{
	bool	found;
	int		i;
	TransactionId *xip_base=NULL;

	/* Create or attach to the SharedSnapshot shared structure */
	sharedSnapshotArray = (SharedSnapshotStruct *)
		ShmemInitStruct("Shared Snapshot", SharedSnapshotShmemSize(), &found);

	Assert(slotCount != 0);
	Assert(xipEntryCount != 0);

	if (!found)
	{
		/*
		 * We're the first - initialize.
		 */
		sharedSnapshotArray->numSlots = 0;

		/* TODO:  MaxBackends is only somewhat right.  What we really want here
		 *        is the MaxBackends value from the QD.  But this is at least
		 *		  safe since we know we dont need *MORE* than MaxBackends.  But
		 *        in general MaxBackends on a QE is going to be bigger than on a
		 *		  QE by a good bit.  or at least it should be.
		 *
		 * But really, max_prepared_transactions *is* what we want (it
		 * corresponds to the number of connections allowed on the
		 * master).
		 *
		 * slotCount is initialized in SharedSnapshotShmemSize().
		 */
		sharedSnapshotArray->maxSlots = slotCount;
		sharedSnapshotArray->nextSlot = 0;

		sharedSnapshotArray->slots = (SharedSnapshotSlot *)&sharedSnapshotArray->xips;

		/* xips start just after the last slot structure */
		xip_base = (TransactionId *)&sharedSnapshotArray->slots[sharedSnapshotArray->maxSlots];

		for (i=0; i < sharedSnapshotArray->maxSlots; i++)
		{
			SharedSnapshotSlot *tmpSlot = &sharedSnapshotArray->slots[i];

			tmpSlot->slotid = -1;
			tmpSlot->slotindex = i;

			/*
			 * Fixup xip array pointer reference space allocated after slot structs:
			 *
			 * Note: xipEntryCount is initialized in SharedSnapshotShmemSize().
			 * So each slot gets (MaxBackends + max_prepared_xacts) transaction-ids.
			 */
			tmpSlot->snapshot.xip = &xip_base[xipEntryCount];
			xip_base += xipEntryCount;
		}
	}
}

/*
 * Used to dump the internal state of the shared slots for debugging.
 */
char *
SharedSnapshotDump(void)
{
	StringInfoData str;
	volatile SharedSnapshotStruct *arrayP = sharedSnapshotArray;
	int			index;

	initStringInfo(&str);

	appendStringInfo(&str, "Local SharedSnapshot Slot Dump: currSlots: %d maxSlots: %d ",
					 arrayP->numSlots, arrayP->maxSlots);

	LWLockAcquire(SharedSnapshotLock, LW_EXCLUSIVE);

	for (index=0; index < arrayP->maxSlots; index++)
	{
		/* need to do byte addressing to find the right slot */
		SharedSnapshotSlot *testSlot = &arrayP->slots[index];

		if (testSlot->slotid != -1)
		{
			appendStringInfo(&str, "(SLOT index: %d slotid: %d QDxid: %u QDcid: %u pid: %u)",
							 testSlot->slotindex, testSlot->slotid, testSlot->QDxid, testSlot->QDcid, (int)testSlot->pid);
		}

	}

	LWLockRelease(SharedSnapshotLock);

	return str.data;
}

/* Acquires an available slot in the sharedSnapshotArray.  The slot is then
 * marked with the supplied slotId.  This slotId is what others will use to
 * find this slot.  This should only ever be called by the "writer" qExec.
 *
 * The slotId should be something that is unique amongst all the possible
 * "writer" qExecs active on a segment database at a given moment.  It also
 * will need to be communicated to the "reader" qExecs so that they can find
 * this slot.
 */
static SharedSnapshotSlot *
SharedSnapshotAdd(int4 slotId)
{
	SharedSnapshotSlot *slot;
	volatile SharedSnapshotStruct *arrayP = sharedSnapshotArray;
	int nextSlot = -1;
	int i;
	int retryCount = gp_snapshotadd_timeout * 10; /* .1 s per wait */

retry:
	LWLockAcquire(SharedSnapshotLock, LW_EXCLUSIVE);

	slot = NULL;

	for (i=0; i < arrayP->maxSlots; i++)
	{
		SharedSnapshotSlot *testSlot = &arrayP->slots[i];

		if (testSlot->slotindex > arrayP->maxSlots)
			elog(ERROR, "Shared Local Snapshots Array appears corrupted: %s", SharedSnapshotDump());

		if (testSlot->slotid == slotId)
		{
			slot = testSlot;
			break;
		}
	}

	if (slot != NULL)
	{
		elog(DEBUG1, "SharedSnapshotAdd: found existing entry for our session-id. id %d retry %d pid %u", slotId, retryCount, (int)slot->pid);
		LWLockRelease(SharedSnapshotLock);

		if (retryCount > 0)
		{
			retryCount--;

			pg_usleep(100000); /* 100ms, wait gp_snapshotadd_timeout seconds max. */
			goto retry;
		}
		else
		{
			insist_log(false, "writer segworker group shared snapshot collision on id %d", slotId);
		}
		/* not reached */
	}

	if (arrayP->numSlots >= arrayP->maxSlots || arrayP->nextSlot == -1)
	{
		/*
		 * Ooops, no room.  this shouldn't happen as something else should have
		 * complained if we go over MaxBackends.
		 */
		LWLockRelease(SharedSnapshotLock);
		ereport(FATAL,
				(errcode(ERRCODE_TOO_MANY_CONNECTIONS),
				 errmsg("sorry, too many clients already."),
				 errdetail("There are no more available slots in the sharedSnapshotArray."),
				 errhint("Another piece of code should have detected that we have too many clients."
						 " this probably means that someone isn't releasing their slot properly.")));
	}

	slot = &arrayP->slots[arrayP->nextSlot];

	slot->slotindex = arrayP->nextSlot;

	/*
	 * find the next available slot
	 */
	for (i=arrayP->nextSlot+1; i < arrayP->maxSlots; i++)
	{
		SharedSnapshotSlot *tmpSlot = &arrayP->slots[i];

		if (tmpSlot->slotid == -1)
		{
			nextSlot = i;
			break;
		}
	}

	/* mark that there isn't a nextSlot if the above loop didn't find one */
	if (nextSlot == arrayP->nextSlot)
		arrayP->nextSlot = -1;
	else
		arrayP->nextSlot = nextSlot;

	arrayP->numSlots += 1;

	/* initialize some things */
	slot->slotid = slotId;
	slot->xid = 0;
	slot->pid = 0;
	slot->cid = 0;
	slot->startTimestamp = 0;
	slot->QDxid = 0;
	slot->QDcid = 0;
	slot->segmateSync = 0;

	LWLockRelease(SharedSnapshotLock);

	return slot;
}

void
GetSlotTableDebugInfo(void **snapshotArray, int *maxSlots)
{
	*snapshotArray = (void *)sharedSnapshotArray;
	*maxSlots = sharedSnapshotArray->maxSlots;
}

/*
 * Used by "reader" qExecs to find the slot in the sharedsnapshotArray with the
 * specified slotId.  In general, we should always be able to find the specified
 * slot unless something unexpected.  If the slot is not found, then NULL is
 * returned.
 *
 * MPP-4599: retry in the same pattern as the writer.
 */
static SharedSnapshotSlot *
SharedSnapshotLookup(int4 slotId)
{
	SharedSnapshotSlot *slot = NULL;
	volatile SharedSnapshotStruct *arrayP = sharedSnapshotArray;
	int retryCount = gp_snapshotadd_timeout * 10; /* .1 s per wait */
	int index;

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		LWLockAcquire(SharedSnapshotLock, LW_SHARED);

		for (index=0; index < arrayP->maxSlots; index++)
		{
			SharedSnapshotSlot *testSlot;

			testSlot = &arrayP->slots[index];

			if (testSlot->slotindex > arrayP->maxSlots)
			{
				LWLockRelease(SharedSnapshotLock);
				elog(ERROR, "Shared Local Snapshots Array appears corrupted: %s", SharedSnapshotDump());
			}

			if (testSlot->slotid == slotId)
			{
				slot = testSlot;
				break;
			}
		}

		LWLockRelease(SharedSnapshotLock);

		if (slot != NULL)
		{
			break;
		}
		else
		{
			if (retryCount > 0)
			{
				retryCount--;

				pg_usleep(100000); /* 100ms, wait gp_snapshotadd_timeout seconds max. */
			}
			else
			{
				break;
			}
		}
	}

	return slot;
}


/*
 * Used by the "writer" qExec to "release" the slot it had been using.
 *
 */
void
SharedSnapshotRemove(volatile SharedSnapshotSlot *slot, char *creatorDescription)
{
	int slotId = slot->slotid;

	LWLockAcquire(SharedSnapshotLock, LW_EXCLUSIVE);

	/* determine if we need to modify the next available slot to use.  we
	 * only do this is our slotindex is lower then the existing one.
	 */
	if (sharedSnapshotArray->nextSlot == -1 || slot->slotindex < sharedSnapshotArray->nextSlot)
	{
		if (slot->slotindex > sharedSnapshotArray->maxSlots)
			elog(ERROR, "Shared Local Snapshots slot has a bogus slotindex: %d. slot array dump: %s",
				 slot->slotindex, SharedSnapshotDump());

		sharedSnapshotArray->nextSlot = slot->slotindex;
	}

	/* reset the slotid which marks it as being unused. */
	slot->slotid = -1;
	slot->xid = 0;
	slot->pid = 0;
	slot->cid = 0;
	slot->startTimestamp = 0;
	slot->QDxid = 0;
	slot->QDcid = 0;
	slot->segmateSync = 0;

	sharedSnapshotArray->numSlots -= 1;

	LWLockRelease(SharedSnapshotLock);

	elog((Debug_print_full_dtm ? LOG : DEBUG5),"SharedSnapshotRemove removed slot for slotId = %d, creator = %s (address %p)",
		 slotId, creatorDescription, SharedLocalSnapshotSlot);
}

void
addSharedSnapshot(char *creatorDescription, int id)
{
	SharedSnapshotSlot *slot;

	slot = SharedSnapshotAdd(id);

	if (slot==NULL)
	{
		ereport(ERROR,
				(errmsg("%s could not set the Shared Local Snapshot!",
						creatorDescription),
				 errdetail("Tried to set the shared local snapshot slot with id: %d "
						   "and failed. Shared Local Snapshots dump: %s", id,
						   SharedSnapshotDump())));
	}
	SharedLocalSnapshotSlot = slot;

	elog((Debug_print_full_dtm ? LOG : DEBUG5),"%s added Shared Local Snapshot slot for gp_session_id = %d (address %p)",
		 creatorDescription, id, SharedLocalSnapshotSlot);
}

void
lookupSharedSnapshot(char *lookerDescription, char *creatorDescription, int id)
{
	SharedSnapshotSlot *slot;

	slot = SharedSnapshotLookup(id);

	if (slot == NULL)
	{
		ereport(ERROR,
				(errmsg("%s could not find Shared Local Snapshot!",
						lookerDescription),
				 errdetail("Tried to find a shared snapshot slot with id: %d "
						   "and found none. Shared Local Snapshots dump: %s", id,
						   SharedSnapshotDump()),
				 errhint("Either this %s was created before the %s or the %s died.",
						 lookerDescription, creatorDescription, creatorDescription)));
	}
	SharedLocalSnapshotSlot = slot;

	elog((Debug_print_full_dtm ? LOG : DEBUG5),"%s found Shared Local Snapshot slot for gp_session_id = %d created by %s (address %p)",
		 lookerDescription, id, creatorDescription, SharedLocalSnapshotSlot);
}

static char *
sharedLocalSnapshot_filename(TransactionId xid, CommandId cid, uint32 segmateSync)
{
	int pid;
	static char filename[MAXPGPATH];
	
	if (Gp_is_writer)
	{
		pid = MyProc->pid;
	}
	else
	{
		if (lockHolderProcPtr == NULL)
		{
			/* get lockholder! */
			elog(ERROR, "NO LOCK HOLDER POINTER.");
		}
		pid = lockHolderProcPtr->pid;
	}

	snprintf(filename, sizeof(filename), "sess%u_w%u_qdxid%u_qdcid%u_sync%u",
			 gp_session_id, pid, xid, cid, segmateSync);
	return filename;
}

/*
 * Dump the shared local snapshot, so that the readers can pick it up.
 *
 * BufFileCreateTemp_ReaderWriter(filename, iswriter)
 */
void
dumpSharedLocalSnapshot_forCursor(void)
{
	SharedSnapshotSlot *src = NULL;
	char* fname = NULL;
	BufFile *f = NULL;
	Size count=0;
	TransactionId *xids = NULL;
	int64 sub_size;
	int64 size_read;
	ResourceOwner oldowner;

	Assert(Gp_role == GP_ROLE_DISPATCH || (Gp_role == GP_ROLE_EXECUTE && Gp_is_writer));
	Assert(SharedLocalSnapshotSlot != NULL);

	src = (SharedSnapshotSlot *)SharedLocalSnapshotSlot;
	fname = sharedLocalSnapshot_filename(src->QDxid, src->QDcid, src->segmateSync);

	/*
	 * Create our dump-file. Hold the reference to it in
	 * the transaction's resource owner, so that it lives as long
	 * as the cursor we're declaring.
	 */
	oldowner = CurrentResourceOwner;
	CurrentResourceOwner = CurTransactionResourceOwner;
	f = BufFileCreateTemp_ReaderWriter(fname, true, false);
	CurrentResourceOwner = oldowner;
	/* we have our file. */

#define FileWriteOK(file, ptr, size) (BufFileWrite(file, ptr, size) == size)

#define FileWriteFieldWithCount(count, file, field) \
    if (BufFileWrite((file), &(field), sizeof(field)) != sizeof(field)) break; \
    count += sizeof(field);

	do
	{
		/* Write our length as zero. (we'll fix it later). */
		count = 0;

		/*
		 * We write two counts here: One is count of first part,
		 * second is size of subtransaction xids copied from
		 * SharedLocalSnapshotSlot. This can be a big number.
		 */
		FileWriteFieldWithCount(count, f, count);
		FileWriteFieldWithCount(count, f, src->total_subcnt);

		FileWriteFieldWithCount(count, f, src->pid);
		FileWriteFieldWithCount(count, f, src->xid);
		FileWriteFieldWithCount(count, f, src->cid);
		FileWriteFieldWithCount(count, f, src->startTimestamp);

		FileWriteFieldWithCount(count, f, src->combocidcnt);
		FileWriteFieldWithCount(count, f, src->combocids);
		FileWriteFieldWithCount(count, f, src->snapshot.xmin);
		FileWriteFieldWithCount(count, f, src->snapshot.xmax);
		FileWriteFieldWithCount(count, f, src->snapshot.xcnt);

		if (!FileWriteOK(f, &src->snapshot.xip, src->snapshot.xcnt * sizeof(TransactionId)))
			break;
		count += src->snapshot.xcnt * sizeof(TransactionId);

		FileWriteFieldWithCount(count, f, src->snapshot.curcid);

		/*
		 * THE STUFF IN THE SHARED LOCAL VERSION OF
		 * snapshot.distribSnapshotWithLocalMapping
		 * APPEARS TO *NEVER* BE USED, SO THERE IS
		 * NO POINT IN TRYING TO DUMP IT (IN FACT,
		 * IT'S ALLOCATION STRATEGY ISN'T SHMEM-FRIENDLY).
		 */

		/*
		 * THIS STUFF IS USED IN THE FILENAME
		 * SO THE READER ALREADY HAS IT.
		 *

		 dst->QDcid = src->QDcid;
		 dst->segmateSync = src->segmateSync;
		 dst->QDxid = src->QDxid;
		 dst->ready = src->ready;

		 *
		 */

		if (src->total_subcnt > src->inmemory_subcnt)
		{
			Assert(subxip_file != 0);

			xids = palloc(MAX_XIDBUF_SIZE);

			FileSeek(subxip_file, 0, SEEK_SET);
			sub_size = (src->total_subcnt - src->inmemory_subcnt)
				    * sizeof(TransactionId);
			while (sub_size > 0)
			{
				size_read = (sub_size > MAX_XIDBUF_SIZE) ?
						MAX_XIDBUF_SIZE : sub_size;
				if (size_read != FileRead(subxip_file, (char *)xids,
							  size_read))
				{
					elog(ERROR,
					     "Error in reading subtransaction file.");
				}

				if (!FileWriteOK(f, xids, sub_size))
				{
					break;
				}

				sub_size -= size_read;
			}

			pfree(xids);
			if (sub_size != 0)
				break;
		}

		if (src->inmemory_subcnt > 0)
		{
			sub_size = src->inmemory_subcnt * sizeof(TransactionId);
			if (!FileWriteOK(f, src->subxids, sub_size))
			{
				break;
			}
		}

		/*
		 * Now update our length field: seek to beginning and overwrite
		 * our original zero-length. count does not include
		 * subtransaction ids.
		 */
		if (BufFileSeek(f, 0 /* offset */, SEEK_SET) != 0)
			break;

		if (!FileWriteOK(f, &count, sizeof(count)))
			break;

		/* now flush and close. */
		BufFileFlush(f);
		/*
		 * Temp files get deleted on close!
		 *
		 * BufFileClose(f);
		 */

		return;
	}
	while (0);

	elog(ERROR, "Failed to write shared snapshot to temp-file");
}

void
readSharedLocalSnapshot_forCursor(Snapshot snapshot)
{
	BufFile *f;
	char *fname=NULL;
	Size count=0, sanity;
	uint8 *p, *buffer=NULL;

	pid_t writerPid;
	TransactionId localXid;
	CommandId localCid;
	TimestampTz localXactStartTimestamp;

	uint32 combocidcnt;
	ComboCidKeyData tmp_combocids[MaxComboCids];
	uint32 sub_size;
	uint32 read_size;
	int64 subcnt;
	TransactionId *subxids = NULL;

	Assert(Gp_role == GP_ROLE_EXECUTE);
	Assert(!Gp_is_writer);
	Assert(SharedLocalSnapshotSlot != NULL);
	Assert(snapshot->xip != NULL);
	Assert(snapshot->subxip != NULL);

	/*
	 * Open our dump-file, this will either return a valid file, or
	 * throw an error.
	 *
	 * NOTE: this is always run *after* the dump by the writer is
	 * guaranteed to have completed.
	 */
	fname = sharedLocalSnapshot_filename(QEDtxContextInfo.distributedXid,
		QEDtxContextInfo.curcid, QEDtxContextInfo.segmateSync);

	f = BufFileCreateTemp_ReaderWriter(fname, false, false);
	/* we have our file. */

#define FileReadOK(file, ptr, size) (BufFileRead(file, ptr, size) == size)

	/* Read the file-length info */
	if (!FileReadOK(f, &count, sizeof(count)))
		elog(ERROR, "Cursor snapshot: failed to read size");

	elog(DEBUG1, "Reading in cursor-snapshot %u bytes",
		     (unsigned int)count);

	buffer = palloc(count);

	/*
	 * Seek back to the beginning:
	 * We're going to read this all in one go, the size
	 * of this buffer should be more than a few hundred bytes.
	 */
	if (BufFileSeek(f, 0 /* offset */, SEEK_SET) != 0)
		elog(ERROR, "Cursor snapshot: failed to seek.");

	if (!FileReadOK(f, buffer, count))
		elog(ERROR, "Cursor snapshot: failed to read content");

	/* we've got the entire snapshot read into our buffer. */
	p = buffer;

	/* sanity check count */
	memcpy(&sanity, p, sizeof(sanity));
	if (sanity != count)
		elog(ERROR, "cursor snapshot failed sanity %u != %u",
			    (unsigned int)sanity, (unsigned int)count);
	p += sizeof(sanity);

	memcpy(&sub_size, p, sizeof(uint32));
	p += sizeof(uint32);

	/* see dumpSharedLocalSnapshot_forCursor() for the correct order here */

	memcpy(&writerPid, p, sizeof(writerPid));
	p += sizeof(writerPid);

	memcpy(&localXid, p, sizeof(localXid));
	p += sizeof(localXid);

	memcpy(&localCid, p, sizeof(localCid));
	p += sizeof(localCid);

	memcpy(&localXactStartTimestamp, p, sizeof(localXactStartTimestamp));
	p += sizeof(localXactStartTimestamp);

	memcpy(&combocidcnt, p, sizeof(combocidcnt));
	p += sizeof(combocidcnt);

	memcpy(tmp_combocids, p, sizeof(tmp_combocids));
	p += sizeof(tmp_combocids);

	/* handle the combocid stuff (same as in GetSnapshotData()) */
	if (usedComboCids != combocidcnt)
	{
		if (usedComboCids == 0)
		{
			MemoryContext oldCtx =  MemoryContextSwitchTo(TopTransactionContext);
			comboCids = palloc(combocidcnt * sizeof(ComboCidKeyData));
			MemoryContextSwitchTo(oldCtx);
		}
		else
			repalloc(comboCids, combocidcnt * sizeof(ComboCidKeyData));
	}
	memcpy(comboCids, tmp_combocids, combocidcnt * sizeof(ComboCidKeyData));
	usedComboCids = ((combocidcnt < MaxComboCids) ? combocidcnt : MaxComboCids);

	memcpy(&snapshot->xmin, p, sizeof(snapshot->xmin));
	p += sizeof(snapshot->xmin);

	memcpy(&snapshot->xmax, p, sizeof(snapshot->xmax));
	p += sizeof(snapshot->xmax);

	memcpy(&snapshot->xcnt, p, sizeof(snapshot->xcnt));
	p += sizeof(snapshot->xcnt);

	memcpy(snapshot->xip, p, snapshot->xcnt * sizeof(TransactionId));
	p += snapshot->xcnt * sizeof(TransactionId);

	/* zero out the slack in the xip-array */
	memset(snapshot->xip + snapshot->xcnt, 0, (xipEntryCount - snapshot->xcnt)*sizeof(TransactionId));

	memcpy(&snapshot->curcid, p, sizeof(snapshot->curcid));

	/* Now we're done with the buffer */
	pfree(buffer);

	/*
	 * Now read the subtransaction ids. This can be a big number, so cannot
	 * allocate memory all at once.
	 */
	sub_size *= sizeof(TransactionId);

	ResetXidBuffer(&subxbuf);

	if (sub_size)
	{
		subxids = palloc(MAX_XIDBUF_SIZE);
	}

	while (sub_size > 0)
	{
		read_size = sub_size > MAX_XIDBUF_SIZE ? MAX_XIDBUF_SIZE : sub_size;
		if (!FileReadOK(f, (char *)subxids, read_size))
		{
			elog(ERROR, "Error in Reading Subtransaction file.");
		}
		subcnt = read_size/sizeof(TransactionId);
		AddSortedToXidBuffer(&subxbuf, subxids, subcnt);
		sub_size -= read_size;
	}

	if (subxids)
	{
		pfree(subxids);
	}

	/* we're done with file. */
	BufFileClose(f);

	SetSharedTransactionId_reader(localXid, snapshot->curcid);

	return;
}

/*
 * Set the buffer dirty after setting t_infomask
 */
static inline void
markDirty(Buffer buffer, Relation relation, HeapTupleHeader tuple, bool isXmin)
{
	TransactionId xid;

	if (!gp_disable_tuple_hints)
	{
		SetBufferCommitInfoNeedsSave(buffer);
		return;
	}

	/*
	 * The GUC gp_disable_tuple_hints is on.  Do further evaluation whether we want to write out the
	 * buffer or not.
	 */
	if (relation == NULL)
	{
		SetBufferCommitInfoNeedsSave(buffer);
		return;
	}

	if (relation->rd_issyscat)
	{
		/* Assume we want to always mark the buffer dirty */
		SetBufferCommitInfoNeedsSave(buffer);
		return;
	}

	/*
	 * Get the xid whose hint bits were just set.
	 */
	if (isXmin)
		xid = HeapTupleHeaderGetXmin(tuple);
	else
		xid = HeapTupleHeaderGetXmax(tuple);

	if (xid == InvalidTransactionId)
	{
		SetBufferCommitInfoNeedsSave(buffer);
		return;
	}

	/*
	 * Check age of the affected xid.  If it is too old, mark the buffer to be written.
	 */
	if (CLOGTransactionIsOld(xid))
	{
		SetBufferCommitInfoNeedsSave(buffer);
		return;
	}
}

/*
 * SetHintBits()
 *
 * Set commit/abort hint bits on a tuple, if appropriate at this time.
 *
 * It is only safe to set a transaction-committed hint bit if we know the
 * transaction's commit record has been flushed to disk.  We cannot change
 * the LSN of the page here because we may hold only a share lock on the
 * buffer, so we can't use the LSN to interlock this; we have to just refrain
 * from setting the hint bit until some future re-examination of the tuple.
 *
 * We can always set hint bits when marking a transaction aborted.	(Some
 * code in heapam.c relies on that!)
 *
 * Also, if we are cleaning up HEAP_MOVED_IN or HEAP_MOVED_OFF entries, then
 * we can always set the hint bits, since VACUUM FULL always uses synchronous
 * commits and doesn't move tuples that weren't previously hinted.	(This is
 * not known by this subroutine, but is applied by its callers.)
 *
 * Normal commits may be asynchronous, so for those we need to get the LSN
 * of the transaction and then check whether this is flushed.
 *
 * The caller should pass xid as the XID of the transaction to check, or
 * InvalidTransactionId if no check is needed.
 */
static inline void
SetHintBits(HeapTupleHeader tuple, Buffer buffer, Relation rel,
			uint16 infomask, TransactionId xid)
{
	bool		isXmin;

	if (TransactionIdIsValid(xid))
	{
		/* NB: xid must be known committed here! */
		XLogRecPtr	commitLSN = TransactionIdGetCommitLSN(xid);

		if (XLogNeedsFlush(commitLSN))
			return;				/* not flushed yet, so don't set hint */
	}

	tuple->t_infomask |= infomask;

	switch(infomask)
	{
		case HEAP_XMIN_INVALID:
		case HEAP_XMIN_COMMITTED:
			isXmin = true;
			break;
		case HEAP_XMAX_INVALID:
		case HEAP_XMAX_COMMITTED:
			isXmin = false;
			break;
		default:
			elog(ERROR, "unexpected infomask while setting hint bits: %d", infomask);
			isXmin = false; /* keep compiler quiet */
	}

	markDirty(buffer, rel, tuple, isXmin);
}

/*
 * HeapTupleSetHintBits --- exported version of SetHintBits()
 *
 * This must be separate because of C99's brain-dead notions about how to
 * implement inline functions.
 */
void
HeapTupleSetHintBits(HeapTupleHeader tuple, Buffer buffer, Relation rel,
					 uint16 infomask, TransactionId xid)
{
	SetHintBits(tuple, buffer, rel, infomask, xid);
}

/*
 * HeapTupleSatisfiesSelf
 *		True iff heap tuple is valid "for itself".
 *
 *	Here, we consider the effects of:
 *		all committed transactions (as of the current instant)
 *		previous commands of this transaction
 *		changes made by the current command
 *
 * Note:
 *		Assumes heap tuple is valid.
 *
 * The satisfaction of "itself" requires the following:
 *
 * ((Xmin == my-transaction &&				the row was updated by the current transaction, and
 *		(Xmax is null						it was not deleted
 *		 [|| Xmax != my-transaction)])			[or it was deleted by another transaction]
 * ||
 *
 * (Xmin is committed &&					the row was modified by a committed transaction, and
 *		(Xmax is null ||					the row has not been deleted, or
 *			(Xmax != my-transaction &&			the row was deleted by another transaction
 *			 Xmax is not committed)))			that has not been committed
 */
bool
HeapTupleSatisfiesSelf(Relation relation, HeapTupleHeader tuple, Snapshot snapshot, Buffer buffer)
{
	if (!(tuple->t_infomask & HEAP_XMIN_COMMITTED))
	{
		if (tuple->t_infomask & HEAP_XMIN_INVALID)
			return false;

		if (tuple->t_infomask & HEAP_MOVED_OFF)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return false;
			if (!TransactionIdIsInProgress(xvac))
			{
				if (TransactionIdDidCommit(xvac))
				{
					SetHintBits(tuple, buffer, relation, HEAP_XMIN_INVALID,
								InvalidTransactionId);
					return false;
				}
				SetHintBits(tuple, buffer, relation, HEAP_XMIN_COMMITTED,
							InvalidTransactionId);
			}
		}
		else if (tuple->t_infomask & HEAP_MOVED_IN)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (!TransactionIdIsCurrentTransactionId(xvac))
			{
				if (TransactionIdIsInProgress(xvac))
					return false;
				if (TransactionIdDidCommit(xvac))
					SetHintBits(tuple, buffer, relation, HEAP_XMIN_COMMITTED,
								InvalidTransactionId);
				else
				{
					SetHintBits(tuple, buffer, relation, HEAP_XMIN_INVALID,
								InvalidTransactionId);
					return false;
				}
			}
		}
		else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(tuple)))
		{
			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
				return true;

			if (tuple->t_infomask & HEAP_IS_LOCKED)		/* not deleter */
				return true;

			Assert(!(tuple->t_infomask & HEAP_XMAX_IS_MULTI));

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmax(tuple)))
			{
				/* deleting subtransaction must have aborted */
				SetHintBits(tuple, buffer, relation, HEAP_XMAX_INVALID,
							InvalidTransactionId);
				return true;
			}

			return false;
		}
		else if (TransactionIdIsInProgress(HeapTupleHeaderGetXmin(tuple)))
			return false;
		else if (TransactionIdDidCommit(HeapTupleHeaderGetXmin(tuple)))
			SetHintBits(tuple, buffer, relation, HEAP_XMIN_COMMITTED,
						HeapTupleHeaderGetXmin(tuple));
		else
		{
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, relation, HEAP_XMIN_INVALID,
						InvalidTransactionId);
			return false;
		}
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
		return true;

	if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
	{
		if (tuple->t_infomask & HEAP_IS_LOCKED)
			return true;
		return false;			/* updated by other */
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		/* MultiXacts are currently only allowed to lock tuples */
		Assert(tuple->t_infomask & HEAP_IS_LOCKED);
		return true;
	}

	if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmax(tuple)))
	{
		if (tuple->t_infomask & HEAP_IS_LOCKED)
			return true;
		return false;
	}

	if (TransactionIdIsInProgress(HeapTupleHeaderGetXmax(tuple)))
		return true;

	if (!TransactionIdDidCommit(HeapTupleHeaderGetXmax(tuple)))
	{
		/* it must have aborted or crashed */
		SetHintBits(tuple, buffer, relation, HEAP_XMAX_INVALID,
					InvalidTransactionId);
		return true;
	}

	/* xmax transaction committed */

	if (tuple->t_infomask & HEAP_IS_LOCKED)
	{
		SetHintBits(tuple, buffer, relation, HEAP_XMAX_INVALID,
					InvalidTransactionId);
		return true;
	}

	SetHintBits(tuple, buffer, relation, HEAP_XMAX_COMMITTED,
				HeapTupleHeaderGetXmax(tuple));
	return false;
}

/*
 * HeapTupleSatisfiesNow
 *		True iff heap tuple is valid "now".
 *
 *	Here, we consider the effects of:
 *		all committed transactions (as of the current instant)
 *		previous commands of this transaction
 *
 * Note we do _not_ include changes made by the current command.  This
 * solves the "Halloween problem" wherein an UPDATE might try to re-update
 * its own output tuples.
 *
 * Note:
 *		Assumes heap tuple is valid.
 *
 * The satisfaction of "now" requires the following:
 *
 * ((Xmin == my-transaction &&				inserted by the current transaction
 *	 Cmin < my-command &&					before this command, and
 *	 (Xmax is null ||						the row has not been deleted, or
 *	  (Xmax == my-transaction &&			it was deleted by the current transaction
 *	   Cmax >= my-command)))				but not before this command,
 * ||										or
 *	(Xmin is committed &&					the row was inserted by a committed transaction, and
 *		(Xmax is null ||					the row has not been deleted, or
 *		 (Xmax == my-transaction &&			the row is being deleted by this transaction
 *		  Cmax >= my-command) ||			but it's not deleted "yet", or
 *		 (Xmax != my-transaction &&			the row was deleted by another transaction
 *		  Xmax is not committed))))			that has not been committed
 *
 *		mao says 17 march 1993:  the tests in this routine are correct;
 *		if you think they're not, you're wrong, and you should think
 *		about it again.  i know, it happened to me.  we don't need to
 *		check commit time against the start time of this transaction
 *		because 2ph locking protects us from doing the wrong thing.
 *		if you mess around here, you'll break serializability.  the only
 *		problem with this code is that it does the wrong thing for system
 *		catalog updates, because the catalogs aren't subject to 2ph, so
 *		the serializability guarantees we provide don't extend to xacts
 *		that do catalog accesses.  this is unfortunate, but not critical.
 */
bool
HeapTupleSatisfiesNow(Relation relation, HeapTupleHeader tuple, Snapshot snapshot, Buffer buffer)
{
	if (!(tuple->t_infomask & HEAP_XMIN_COMMITTED))
	{
		if (tuple->t_infomask & HEAP_XMIN_INVALID)
			return false;

		if (tuple->t_infomask & HEAP_MOVED_OFF)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return false;
			if (!TransactionIdIsInProgress(xvac))
			{
				if (TransactionIdDidCommit(xvac))
				{
					SetHintBits(tuple, buffer, relation, HEAP_XMIN_INVALID,
								InvalidTransactionId);
					return false;
				}
				SetHintBits(tuple, buffer, relation, HEAP_XMIN_COMMITTED,
							InvalidTransactionId);
			}
		}
		else if (tuple->t_infomask & HEAP_MOVED_IN)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (!TransactionIdIsCurrentTransactionId(xvac))
			{
				if (TransactionIdIsInProgress(xvac))
					return false;
				if (TransactionIdDidCommit(xvac))
					SetHintBits(tuple, buffer, relation, HEAP_XMIN_COMMITTED,
								InvalidTransactionId);
				else
				{
					SetHintBits(tuple, buffer, relation, HEAP_XMIN_INVALID,
								InvalidTransactionId);
					return false;
				}
			}
		}
		else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(tuple)))
		{
			if (HeapTupleHeaderGetCmin(tuple) >= GetCurrentCommandId(false))
				return false;	/* inserted after scan started */

			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
				return true;

			if (tuple->t_infomask & HEAP_IS_LOCKED)		/* not deleter */
				return true;

			Assert(!(tuple->t_infomask & HEAP_XMAX_IS_MULTI));

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmax(tuple)))
			{
				/* deleting subtransaction must have aborted */
				SetHintBits(tuple, buffer, relation, HEAP_XMAX_INVALID,
							InvalidTransactionId);
				return true;
			}

			if (HeapTupleHeaderGetCmax(tuple) >= GetCurrentCommandId(false))
				return true;	/* deleted after scan started */
			else
				return false;	/* deleted before scan started */
		}
		else if (TransactionIdIsInProgress(HeapTupleHeaderGetXmin(tuple)))
			return false;
		else if (TransactionIdDidCommit(HeapTupleHeaderGetXmin(tuple)))
			SetHintBits(tuple, buffer, relation, HEAP_XMIN_COMMITTED,
						HeapTupleHeaderGetXmin(tuple));
		else
		{
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, relation, HEAP_XMIN_INVALID,
						InvalidTransactionId);
			return false;
		}
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
		return true;

	if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
	{
		if (tuple->t_infomask & HEAP_IS_LOCKED)
			return true;
		return false;
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		/* MultiXacts are currently only allowed to lock tuples */
		Assert(tuple->t_infomask & HEAP_IS_LOCKED);
		return true;
	}

	if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmax(tuple)))
	{
		if (tuple->t_infomask & HEAP_IS_LOCKED)
			return true;
		if (HeapTupleHeaderGetCmax(tuple) >= GetCurrentCommandId(false))
			return true;		/* deleted after scan started */
		else
			return false;		/* deleted before scan started */
	}

	if (TransactionIdIsInProgress(HeapTupleHeaderGetXmax(tuple)))
		return true;

	if (!TransactionIdDidCommit(HeapTupleHeaderGetXmax(tuple)))
	{
		/* it must have aborted or crashed */
		SetHintBits(tuple, buffer, relation, HEAP_XMAX_INVALID,
					InvalidTransactionId);
		return true;
	}

	/* xmax transaction committed */

	if (tuple->t_infomask & HEAP_IS_LOCKED)
	{
		SetHintBits(tuple, buffer, relation, HEAP_XMAX_INVALID,
					InvalidTransactionId);
		return true;
	}

	SetHintBits(tuple, buffer, relation, HEAP_XMAX_COMMITTED,
				HeapTupleHeaderGetXmax(tuple));
	return false;
}

/*
 * HeapTupleSatisfiesAny
 *		Dummy "satisfies" routine: any tuple satisfies SnapshotAny.
 */
bool
HeapTupleSatisfiesAny(Relation relation, HeapTupleHeader tuple, Snapshot snapshot, Buffer buffer)
{
	return true;
}

/*
 * HeapTupleSatisfiesToast
 *		True iff heap tuple is valid as a TOAST row.
 *
 * This is a simplified version that only checks for VACUUM moving conditions.
 * It's appropriate for TOAST usage because TOAST really doesn't want to do
 * its own time qual checks; if you can see the main table row that contains
 * a TOAST reference, you should be able to see the TOASTed value.	However,
 * vacuuming a TOAST table is independent of the main table, and in case such
 * a vacuum fails partway through, we'd better do this much checking.
 *
 * Among other things, this means you can't do UPDATEs of rows in a TOAST
 * table.
 */
bool
HeapTupleSatisfiesToast(Relation relation, HeapTupleHeader tuple, Snapshot snapshot,
						Buffer buffer)
{
	if (!(tuple->t_infomask & HEAP_XMIN_COMMITTED))
	{
		if (tuple->t_infomask & HEAP_XMIN_INVALID)
			return false;

		if (tuple->t_infomask & HEAP_MOVED_OFF)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return false;
			if (!TransactionIdIsInProgress(xvac))
			{
				if (TransactionIdDidCommit(xvac))
				{
					SetHintBits(tuple, buffer, relation, HEAP_XMIN_INVALID,
								InvalidTransactionId);
					return false;
				}
				SetHintBits(tuple, buffer, relation, HEAP_XMIN_COMMITTED,
							InvalidTransactionId);
			}
		}
		else if (tuple->t_infomask & HEAP_MOVED_IN)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (!TransactionIdIsCurrentTransactionId(xvac))
			{
				if (TransactionIdIsInProgress(xvac))
					return false;
				if (TransactionIdDidCommit(xvac))
					SetHintBits(tuple, buffer, relation, HEAP_XMIN_COMMITTED,
								InvalidTransactionId);
				else
				{
					SetHintBits(tuple, buffer, relation, HEAP_XMIN_INVALID,
								InvalidTransactionId);
					return false;
				}
			}
		}
	}

	/* otherwise assume the tuple is valid for TOAST. */
	return true;
}

/*
 * HeapTupleSatisfiesUpdate
 *
 *	Same logic as HeapTupleSatisfiesNow, but returns a more detailed result
 *	code, since UPDATE needs to know more than "is it visible?".  Also,
 *	tuples of my own xact are tested against the passed CommandId not
 *	CurrentCommandId.
 *
 *	The possible return codes are:
 *
 *	HeapTupleInvisible: the tuple didn't exist at all when the scan started,
 *	e.g. it was created by a later CommandId.
 *
 *	HeapTupleMayBeUpdated: The tuple is valid and visible, so it may be
 *	updated.
 *
 *	HeapTupleSelfUpdated: The tuple was updated by the current transaction,
 *	after the current scan started.
 *
 *	HeapTupleUpdated: The tuple was updated by a committed transaction.
 *
 *	HeapTupleBeingUpdated: The tuple is being updated by an in-progress
 *	transaction other than the current transaction.  (Note: this includes
 *	the case where the tuple is share-locked by a MultiXact, even if the
 *	MultiXact includes the current transaction.  Callers that want to
 *	distinguish that case must test for it themselves.)
 */
HTSU_Result
HeapTupleSatisfiesUpdate(Relation relation, HeapTupleHeader tuple, CommandId curcid,
						 Buffer buffer)
{
	if (!(tuple->t_infomask & HEAP_XMIN_COMMITTED))
	{
		if (tuple->t_infomask & HEAP_XMIN_INVALID)
			return HeapTupleInvisible;

		if (tuple->t_infomask & HEAP_MOVED_OFF)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return HeapTupleInvisible;
			if (!TransactionIdIsInProgress(xvac))
			{
				if (TransactionIdDidCommit(xvac))
				{
					SetHintBits(tuple, buffer, relation, HEAP_XMIN_INVALID,
								InvalidTransactionId);
					return HeapTupleInvisible;
				}
				SetHintBits(tuple, buffer, relation, HEAP_XMIN_COMMITTED,
							InvalidTransactionId);
			}
		}
		else if (tuple->t_infomask & HEAP_MOVED_IN)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (!TransactionIdIsCurrentTransactionId(xvac))
			{
				if (TransactionIdIsInProgress(xvac))
					return HeapTupleInvisible;
				if (TransactionIdDidCommit(xvac))
					SetHintBits(tuple, buffer, relation, HEAP_XMIN_COMMITTED,
								InvalidTransactionId);
				else
				{
					SetHintBits(tuple, buffer, relation, HEAP_XMIN_INVALID,
								InvalidTransactionId);
					return HeapTupleInvisible;
				}
			}
		}
		else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(tuple)))
		{
			if (HeapTupleHeaderGetCmin(tuple) >= curcid)
				return HeapTupleInvisible;		/* inserted after scan started */

			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
				return HeapTupleMayBeUpdated;

			if (tuple->t_infomask & HEAP_IS_LOCKED)		/* not deleter */
				return HeapTupleMayBeUpdated;

			Assert(!(tuple->t_infomask & HEAP_XMAX_IS_MULTI));

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmax(tuple)))
			{
				/* deleting subtransaction must have aborted */
				SetHintBits(tuple, buffer, relation, HEAP_XMAX_INVALID,
							InvalidTransactionId);
				return HeapTupleMayBeUpdated;
			}

			if (HeapTupleHeaderGetCmax(tuple) >= curcid)
				return HeapTupleSelfUpdated;	/* updated after scan started */
			else
				return HeapTupleInvisible;		/* updated before scan started */
		}
		else if (TransactionIdIsInProgress(HeapTupleHeaderGetXmin(tuple)))
			return HeapTupleInvisible;
		else if (TransactionIdDidCommit(HeapTupleHeaderGetXmin(tuple)))
			SetHintBits(tuple, buffer, relation, HEAP_XMIN_COMMITTED,
						HeapTupleHeaderGetXmin(tuple));
		else
		{
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, relation, HEAP_XMIN_INVALID,
						InvalidTransactionId);
			return HeapTupleInvisible;
		}
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
		return HeapTupleMayBeUpdated;

	if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
	{
		if (tuple->t_infomask & HEAP_IS_LOCKED)
			return HeapTupleMayBeUpdated;
		return HeapTupleUpdated;	/* updated by other */
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		/* MultiXacts are currently only allowed to lock tuples */
		Assert(tuple->t_infomask & HEAP_IS_LOCKED);

		if (MultiXactIdIsRunning(HeapTupleHeaderGetXmax(tuple)))
			return HeapTupleBeingUpdated;
		SetHintBits(tuple, buffer, relation, HEAP_XMAX_INVALID,
					InvalidTransactionId);
		return HeapTupleMayBeUpdated;
	}

	if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmax(tuple)))
	{
		if (tuple->t_infomask & HEAP_IS_LOCKED)
			return HeapTupleMayBeUpdated;
		if (HeapTupleHeaderGetCmax(tuple) >= curcid)
			return HeapTupleSelfUpdated;		/* updated after scan started */
		else
			return HeapTupleInvisible;	/* updated before scan started */
	}

	if (TransactionIdIsInProgress(HeapTupleHeaderGetXmax(tuple)))
		return HeapTupleBeingUpdated;

	if (!TransactionIdDidCommit(HeapTupleHeaderGetXmax(tuple)))
	{
		/* it must have aborted or crashed */
		SetHintBits(tuple, buffer, relation, HEAP_XMAX_INVALID,
					InvalidTransactionId);
		return HeapTupleMayBeUpdated;
	}

	/* xmax transaction committed */

	if (tuple->t_infomask & HEAP_IS_LOCKED)
	{
		SetHintBits(tuple, buffer, relation, HEAP_XMAX_INVALID,
					InvalidTransactionId);
		return HeapTupleMayBeUpdated;
	}

	SetHintBits(tuple, buffer, relation, HEAP_XMAX_COMMITTED,
				HeapTupleHeaderGetXmax(tuple));
	return HeapTupleUpdated;	/* updated by other */
}

/*
 * HeapTupleSatisfiesDirty
 *		True iff heap tuple is valid including effects of open transactions.
 *
 *	Here, we consider the effects of:
 *		all committed and in-progress transactions (as of the current instant)
 *		previous commands of this transaction
 *		changes made by the current command
 *
 * This is essentially like HeapTupleSatisfiesSelf as far as effects of
 * the current transaction and committed/aborted xacts are concerned.
 * However, we also include the effects of other xacts still in progress.
 *
 * A special hack is that the passed-in snapshot struct is used as an
 * output argument to return the xids of concurrent xacts that affected the
 * tuple.  snapshot->xmin is set to the tuple's xmin if that is another
 * transaction that's still in progress; or to InvalidTransactionId if the
 * tuple's xmin is committed good, committed dead, or my own xact.  Similarly
 * for snapshot->xmax and the tuple's xmax.
 */
bool
HeapTupleSatisfiesDirty(Relation relation, HeapTupleHeader tuple, Snapshot snapshot,
						Buffer buffer)
{
	snapshot->xmin = snapshot->xmax = InvalidTransactionId;

	if (!(tuple->t_infomask & HEAP_XMIN_COMMITTED))
	{
		if (tuple->t_infomask & HEAP_XMIN_INVALID)
			return false;

		if (tuple->t_infomask & HEAP_MOVED_OFF)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return false;
			if (!TransactionIdIsInProgress(xvac))
			{
				if (TransactionIdDidCommit(xvac))
				{
					SetHintBits(tuple, buffer, relation, HEAP_XMIN_INVALID,
								InvalidTransactionId);
					return false;
				}
				SetHintBits(tuple, buffer, relation, HEAP_XMIN_COMMITTED,
							InvalidTransactionId);
			}
		}
		else if (tuple->t_infomask & HEAP_MOVED_IN)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (!TransactionIdIsCurrentTransactionId(xvac))
			{
				if (TransactionIdIsInProgress(xvac))
					return false;
				if (TransactionIdDidCommit(xvac))
					SetHintBits(tuple, buffer, relation, HEAP_XMIN_COMMITTED,
								InvalidTransactionId);
				else
				{
					SetHintBits(tuple, buffer, relation, HEAP_XMIN_INVALID,
								InvalidTransactionId);
					return false;
				}
			}
		}
		else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(tuple)))
		{
			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
				return true;

			if (tuple->t_infomask & HEAP_IS_LOCKED)		/* not deleter */
				return true;

			Assert(!(tuple->t_infomask & HEAP_XMAX_IS_MULTI));

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmax(tuple)))
			{
				/* deleting subtransaction must have aborted */
				SetHintBits(tuple, buffer, relation, HEAP_XMAX_INVALID,
							InvalidTransactionId);
				return true;
			}

			return false;
		}
		else if (TransactionIdIsInProgress(HeapTupleHeaderGetXmin(tuple)))
		{
			snapshot->xmin = HeapTupleHeaderGetXmin(tuple);
			/* XXX shouldn't we fall through to look at xmax? */
			return true;		/* in insertion by other */
		}
		else if (TransactionIdDidCommit(HeapTupleHeaderGetXmin(tuple)))
			SetHintBits(tuple, buffer, relation, HEAP_XMIN_COMMITTED,
						HeapTupleHeaderGetXmin(tuple));
		else
		{
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, relation, HEAP_XMIN_INVALID,
						InvalidTransactionId);
			return false;
		}
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
		return true;

	if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
	{
		if (tuple->t_infomask & HEAP_IS_LOCKED)
			return true;
		return false;			/* updated by other */
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		/* MultiXacts are currently only allowed to lock tuples */
		Assert(tuple->t_infomask & HEAP_IS_LOCKED);
		return true;
	}

	if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmax(tuple)))
	{
		if (tuple->t_infomask & HEAP_IS_LOCKED)
			return true;
		return false;
	}

	if (TransactionIdIsInProgress(HeapTupleHeaderGetXmax(tuple)))
	{
		snapshot->xmax = HeapTupleHeaderGetXmax(tuple);
		return true;
	}

	if (!TransactionIdDidCommit(HeapTupleHeaderGetXmax(tuple)))
	{
		/* it must have aborted or crashed */
		SetHintBits(tuple, buffer, relation, HEAP_XMAX_INVALID,
					InvalidTransactionId);
		return true;
	}

	/* xmax transaction committed */

	if (tuple->t_infomask & HEAP_IS_LOCKED)
	{
		SetHintBits(tuple, buffer, relation, HEAP_XMAX_INVALID,
					InvalidTransactionId);
		return true;
	}

	SetHintBits(tuple, buffer, relation, HEAP_XMAX_COMMITTED,
				HeapTupleHeaderGetXmax(tuple));
	return false;				/* updated by other */
}

/*
 * HeapTupleSatisfiesMVCC
 *		True iff heap tuple is valid for the given MVCC snapshot.
 *
 *	Here, we consider the effects of:
 *		all transactions committed as of the time of the given snapshot
 *		previous commands of this transaction
 *
 *	Does _not_ include:
 *		transactions shown as in-progress by the snapshot
 *		transactions started after the snapshot was taken
 *		changes made by the current command
 *
 * This is the same as HeapTupleSatisfiesNow, except that transactions that
 * were in progress or as yet unstarted when the snapshot was taken will
 * be treated as uncommitted, even if they have committed by now.
 *
 * (Notice, however, that the tuple status hint bits will be updated on the
 * basis of the true state of the transaction, even if we then pretend we
 * can't see it.)
 */
bool
HeapTupleSatisfiesMVCC(Relation relation, HeapTupleHeader tuple, Snapshot snapshot,
					   Buffer buffer)
{
	bool inSnapshot = false;
	bool setDistributedSnapshotIgnore = false;

	if (!(tuple->t_infomask & HEAP_XMIN_COMMITTED))
	{
		if (tuple->t_infomask & HEAP_XMIN_INVALID)
			return false;

		if (tuple->t_infomask & HEAP_MOVED_OFF)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return false;
			if (!TransactionIdIsInProgress(xvac))
			{
				if (TransactionIdDidCommit(xvac))
				{
					SetHintBits(tuple, buffer, relation, HEAP_XMIN_INVALID,
								InvalidTransactionId);
					return false;
				}
				SetHintBits(tuple, buffer, relation, HEAP_XMIN_COMMITTED,
							InvalidTransactionId);
			}
		}
		else if (tuple->t_infomask & HEAP_MOVED_IN)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (!TransactionIdIsCurrentTransactionId(xvac))
			{
				if (TransactionIdIsInProgress(xvac))
					return false;
				if (TransactionIdDidCommit(xvac))
					SetHintBits(tuple, buffer, relation, HEAP_XMIN_COMMITTED,
								InvalidTransactionId);
				else
				{
					SetHintBits(tuple, buffer, relation, HEAP_XMIN_INVALID,
								InvalidTransactionId);
					return false;
				}
			}
		}
		else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(tuple)))
		{
			if (HeapTupleHeaderGetCmin(tuple) >= snapshot->curcid)
				return false;	/* inserted after scan started */

			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
				return true;

			if (tuple->t_infomask & HEAP_IS_LOCKED)		/* not deleter */
				return true;

			Assert(!(tuple->t_infomask & HEAP_XMAX_IS_MULTI));

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmax(tuple)))
			{
				/* deleting subtransaction must have aborted */
				SetHintBits(tuple, buffer, relation, HEAP_XMAX_INVALID,
							InvalidTransactionId);
				return true;
			}

			/*
			 * MPP-8317: cursors can't always *tell* that this is the current transaction.
			 */
			Assert(QEDtxContextInfo.cursorContext || TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmax(tuple)));

			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				return true;	/* deleted after scan started */
			else
				return false;	/* deleted before scan started */
		}
		else if (TransactionIdIsInProgress(HeapTupleHeaderGetXmin(tuple)))
			return false;
		else if (TransactionIdDidCommit(HeapTupleHeaderGetXmin(tuple)))
			SetHintBits(tuple, buffer, relation, HEAP_XMIN_COMMITTED,
						HeapTupleHeaderGetXmin(tuple));
		else
		{
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, relation, HEAP_XMIN_INVALID,
						InvalidTransactionId);
			return false;
		}
	}

	/*
	 * By here, the inserting transaction has committed - have to check
	 * when...
	 */
	inSnapshot =
		XidInMVCCSnapshot(HeapTupleHeaderGetXmin(tuple), snapshot,
						  /* isXmax */ false,
						  ((tuple->t_infomask2 & HEAP_XMIN_DISTRIBUTED_SNAPSHOT_IGNORE) != 0),
						  &setDistributedSnapshotIgnore);
	if (setDistributedSnapshotIgnore)
	{
		tuple->t_infomask2 |= HEAP_XMIN_DISTRIBUTED_SNAPSHOT_IGNORE;
		markDirty(buffer, relation, tuple, /* isXmin */ true);
	}

	if (inSnapshot)
		return false;			/* treat as still in progress */

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
		return true;

	if (tuple->t_infomask & HEAP_IS_LOCKED)
		return true;

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		/* MultiXacts are currently only allowed to lock tuples */
		Assert(tuple->t_infomask & HEAP_IS_LOCKED);
		return true;
	}

	if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
	{
		if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmax(tuple)))
		{
			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				return true;	/* deleted after scan started */
			else
				return false;	/* deleted before scan started */
		}

		if (TransactionIdIsInProgress(HeapTupleHeaderGetXmax(tuple)))
			return true;

		if (!TransactionIdDidCommit(HeapTupleHeaderGetXmax(tuple)))
		{
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, relation, HEAP_XMAX_INVALID,
						InvalidTransactionId);
			return true;
		}

		/* xmax transaction committed */
		SetHintBits(tuple, buffer, relation, HEAP_XMAX_COMMITTED,
					HeapTupleHeaderGetXmax(tuple));
	}

	/*
	 * OK, the deleting transaction committed too ... but when?
	 */
	inSnapshot =
			XidInMVCCSnapshot(HeapTupleHeaderGetXmax(tuple), snapshot,
							  /* isXmax */ true,
							  ((tuple->t_infomask2 & HEAP_XMAX_DISTRIBUTED_SNAPSHOT_IGNORE) != 0),
							  &setDistributedSnapshotIgnore);
	if (setDistributedSnapshotIgnore)
	{
		tuple->t_infomask2 |= HEAP_XMAX_DISTRIBUTED_SNAPSHOT_IGNORE;
		markDirty(buffer, relation, tuple, /* isXmin */ false);
	}

	if (inSnapshot)
		return true;			/* treat as still in progress */

	return false;
}

/*
 * HeapTupleSatisfiesVacuum
 *
 *	Determine the status of tuples for VACUUM purposes.  Here, what
 *	we mainly want to know is if a tuple is potentially visible to *any*
 *	running transaction.  If so, it can't be removed yet by VACUUM.
 *
 * OldestXmin is a cutoff XID (obtained from GetOldestXmin()).	Tuples
 * deleted by XIDs >= OldestXmin are deemed "recently dead"; they might
 * still be visible to some open transaction, so we can't remove them,
 * even if we see that the deleting transaction has committed.
 */
HTSV_Result
HeapTupleSatisfiesVacuum(Relation relation, HeapTupleHeader tuple, TransactionId OldestXmin,
						 Buffer buffer)
{
	/*
	 * Has inserting transaction committed?
	 *
	 * If the inserting transaction aborted, then the tuple was never visible
	 * to any other transaction, so we can delete it immediately.
	 */
	if (!(tuple->t_infomask & HEAP_XMIN_COMMITTED))
	{
		if (tuple->t_infomask & HEAP_XMIN_INVALID)
			return HEAPTUPLE_DEAD;
		else if (tuple->t_infomask & HEAP_MOVED_OFF)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return HEAPTUPLE_DELETE_IN_PROGRESS;
			if (TransactionIdIsInProgress(xvac))
				return HEAPTUPLE_DELETE_IN_PROGRESS;
			if (TransactionIdDidCommit(xvac))
			{
				SetHintBits(tuple, buffer, relation, HEAP_XMIN_INVALID,
							InvalidTransactionId);
				return HEAPTUPLE_DEAD;
			}
			SetHintBits(tuple, buffer, relation, HEAP_XMIN_COMMITTED,
						InvalidTransactionId);
		}
		else if (tuple->t_infomask & HEAP_MOVED_IN)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return HEAPTUPLE_INSERT_IN_PROGRESS;
			if (TransactionIdIsInProgress(xvac))
				return HEAPTUPLE_INSERT_IN_PROGRESS;
			if (TransactionIdDidCommit(xvac))
				SetHintBits(tuple, buffer, relation, HEAP_XMIN_COMMITTED,
							InvalidTransactionId);
			else
			{
				SetHintBits(tuple, buffer, relation, HEAP_XMIN_INVALID,
							InvalidTransactionId);
				return HEAPTUPLE_DEAD;
			}
		}
		else if (TransactionIdIsInProgress(HeapTupleHeaderGetXmin(tuple)))
		{
			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
				return HEAPTUPLE_INSERT_IN_PROGRESS;
			if (tuple->t_infomask & HEAP_IS_LOCKED)
				return HEAPTUPLE_INSERT_IN_PROGRESS;
			/* inserted and then deleted by same xact */
			return HEAPTUPLE_DELETE_IN_PROGRESS;
		}
		else if (TransactionIdDidCommit(HeapTupleHeaderGetXmin(tuple)))
			SetHintBits(tuple, buffer, relation, HEAP_XMIN_COMMITTED,
						HeapTupleHeaderGetXmin(tuple));
		else
		{
			/*
			 * Not in Progress, Not Committed, so either Aborted or crashed
			 */
			SetHintBits(tuple, buffer, relation, HEAP_XMIN_INVALID,
						InvalidTransactionId);
			return HEAPTUPLE_DEAD;
		}

		/*
		 * At this point the xmin is known committed, but we might not have
		 * been able to set the hint bit yet; so we can no longer Assert that
		 * it's set.
		 */
	}

	/*
	 * Okay, the inserter committed, so it was good at some point.	Now what
	 * about the deleting transaction?
	 */
	if (tuple->t_infomask & HEAP_XMAX_INVALID)
		return HEAPTUPLE_LIVE;

	if (tuple->t_infomask & HEAP_IS_LOCKED)
	{
		/*
		 * "Deleting" xact really only locked it, so the tuple is live in any
		 * case.  However, we should make sure that either XMAX_COMMITTED or
		 * XMAX_INVALID gets set once the xact is gone, to reduce the costs of
		 * examining the tuple for future xacts.  Also, marking dead
		 * MultiXacts as invalid here provides defense against MultiXactId
		 * wraparound (see also comments in heap_freeze_tuple()).
		 */
		if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
		{
			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			{
				if (MultiXactIdIsRunning(HeapTupleHeaderGetXmax(tuple)))
					return HEAPTUPLE_LIVE;
			}
			else
			{
				if (TransactionIdIsInProgress(HeapTupleHeaderGetXmax(tuple)))
					return HEAPTUPLE_LIVE;
			}

			/*
			 * We don't really care whether xmax did commit, abort or crash.
			 * We know that xmax did lock the tuple, but it did not and will
			 * never actually update it.
			 */
			SetHintBits(tuple, buffer, relation, HEAP_XMAX_INVALID,
						InvalidTransactionId);
		}
		return HEAPTUPLE_LIVE;
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		/* MultiXacts are currently only allowed to lock tuples */
		Assert(tuple->t_infomask & HEAP_IS_LOCKED);
		return HEAPTUPLE_LIVE;
	}

	if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
	{
		if (TransactionIdIsInProgress(HeapTupleHeaderGetXmax(tuple)))
			return HEAPTUPLE_DELETE_IN_PROGRESS;
		else if (TransactionIdDidCommit(HeapTupleHeaderGetXmax(tuple)))
			SetHintBits(tuple, buffer, relation, HEAP_XMAX_COMMITTED,
						HeapTupleHeaderGetXmax(tuple));
		else
		{
			/*
			 * Not in Progress, Not Committed, so either Aborted or crashed
			 */
			SetHintBits(tuple, buffer, relation, HEAP_XMAX_INVALID,
						InvalidTransactionId);
			return HEAPTUPLE_LIVE;
		}

		/*
		 * At this point the xmax is known committed, but we might not have
		 * been able to set the hint bit yet; so we can no longer Assert that
		 * it's set.
		 */
	}

	/*
	 * Deleter committed, but perhaps it was recent enough that some open
	 * transactions could still see the tuple.
	 */
	if (!TransactionIdPrecedes(HeapTupleHeaderGetXmax(tuple), OldestXmin))
		return HEAPTUPLE_RECENTLY_DEAD;

	/* Otherwise, it's dead and removable */
	return HEAPTUPLE_DEAD;
}


/*
 * GetTransactionSnapshot
 *		Get the appropriate snapshot for a new query in a transaction.
 *
 * The SerializableSnapshot is the first one taken in a transaction.
 * In serializable mode we just use that one throughout the transaction.
 * In read-committed mode, we take a new snapshot each time we are called.
 *
 * Note that the return value points at static storage that will be modified
 * by future calls and by CommandCounterIncrement().  Callers should copy
 * the result with CopySnapshot() if it is to be used very long.
 */
Snapshot
GetTransactionSnapshot(void)
{
	/* First call in transaction? */
	if (SerializableSnapshot == NULL)
	{
		SerializableSnapshot = GetSnapshotData(&SerializableSnapshotData, true);
		return SerializableSnapshot;
	}

	if (IsXactIsoLevelSerializable)
	{
		elog((Debug_print_snapshot_dtm ? LOG : DEBUG5),"[Distributed Snapshot #%u] *Serializable Skip* (gxid = %u, '%s')",
			 (SerializableSnapshot == NULL ? 0 : SerializableSnapshot->distribSnapshotWithLocalMapping.header.distribSnapshotId),
			 getDistributedTransactionId(),
			 DtxContextToString(DistributedTransactionContext));

		UpdateSerializableCommandId();

		return SerializableSnapshot;
	}

	elog((Debug_print_snapshot_dtm ? LOG : DEBUG5),"[Distributed Snapshot #%u] (gxid = %u, '%s')",
		 (LatestSnapshot == NULL ? 0 : LatestSnapshot->distribSnapshotWithLocalMapping.header.distribSnapshotId),
		 getDistributedTransactionId(),
		 DtxContextToString(DistributedTransactionContext));

	LatestSnapshot = GetSnapshotData(&LatestSnapshotData, false);

	return LatestSnapshot;
}

/*
 * GetLatestSnapshot
 *		Get a snapshot that is up-to-date as of the current instant,
 *		even if we are executing in SERIALIZABLE mode.
 */
Snapshot
GetLatestSnapshot(void)
{
	/* Should not be first call in transaction */
	if (SerializableSnapshot == NULL)
		elog(ERROR, "no snapshot has been set");

	LatestSnapshot = GetSnapshotData(&LatestSnapshotData, false);

	return LatestSnapshot;
}

/*
 * CopySnapshot
 *		Copy the given snapshot.
 *
 * The copy is palloc'd in the current memory context.
 */
Snapshot
CopySnapshot(Snapshot snapshot)
{
	Snapshot	newsnap;
	Size		subxipoff;
	Size		dsoff = 0;
	Size		size;

	if (!IsMVCCSnapshot(snapshot))
		return snapshot;

	/* We allocate any XID arrays needed in the same palloc block. */
	size = subxipoff = sizeof(SnapshotData) +
		snapshot->xcnt * sizeof(TransactionId);
	if (snapshot->subxcnt > 0)
		size += snapshot->subxcnt * sizeof(TransactionId);

	if (snapshot->haveDistribSnapshot &&
		snapshot->distribSnapshotWithLocalMapping.header.count > 0)
	{
		dsoff = size;
		size += snapshot->distribSnapshotWithLocalMapping.header.count *
			sizeof(DistributedSnapshotMapEntry);
	}

	newsnap = (Snapshot) palloc(size);
	memcpy(newsnap, snapshot, sizeof(SnapshotData));

	/* setup XID array */
	if (snapshot->xcnt > 0)
	{
		newsnap->xip = (TransactionId *) (newsnap + 1);
		memcpy(newsnap->xip, snapshot->xip,
			   snapshot->xcnt * sizeof(TransactionId));
	}
	else
		newsnap->xip = NULL;

	/* setup subXID array */
	if (snapshot->subxcnt > 0)
	{
		newsnap->subxip = (TransactionId *) ((char *) newsnap + subxipoff);
		memcpy(newsnap->subxip, snapshot->subxip,
			   snapshot->subxcnt * sizeof(TransactionId));
	}
	else
		newsnap->subxip = NULL;

	if (snapshot->haveDistribSnapshot &&
		snapshot->distribSnapshotWithLocalMapping.header.count > 0)
	{
		newsnap->distribSnapshotWithLocalMapping.inProgressEntryArray =
			(DistributedSnapshotMapEntry *) ((char *) newsnap + dsoff);
		memcpy(newsnap->distribSnapshotWithLocalMapping.inProgressEntryArray,
			   snapshot->distribSnapshotWithLocalMapping.inProgressEntryArray,
			   snapshot->distribSnapshotWithLocalMapping.header.count *
			   sizeof(DistributedSnapshotMapEntry));
	}
	else
	{
		newsnap->distribSnapshotWithLocalMapping.inProgressEntryArray = NULL;
	}

	return newsnap;
}

/*
 * FreeSnapshot
 *		Free a snapshot previously copied with CopySnapshot.
 *
 * This is currently identical to pfree, but is provided for cleanliness.
 *
 * Do *not* apply this to the results of GetTransactionSnapshot or
 * GetLatestSnapshot, since those are just static structs.
 */
void
FreeSnapshot(Snapshot snapshot)
{
	if (!IsMVCCSnapshot(snapshot))
		return;

	pfree(snapshot);
}

/*
 * FreeXactSnapshot
 *		Free snapshot(s) at end of transaction.
 */
void
FreeXactSnapshot(void)
{
	/*
	 * We do not free the xip arrays for the static snapshot structs; they
	 * will be reused soon. So this is now just a state change to prevent
	 * outside callers from accessing the snapshots.
	 */
	SerializableSnapshot = NULL;
	LatestSnapshot = NULL;
	ActiveSnapshot = NULL;		/* just for cleanliness */
}

/*
 * LogDistributedSnapshotInfo
 *   Log the distributed snapshot info in a given snapshot.
 *
 * The 'prefix' is used to prefix the log message.
 */
void
LogDistributedSnapshotInfo(Snapshot snapshot, const char *prefix)
{
	static const int MESSAGE_LEN = 500;

	if (!IsMVCCSnapshot(snapshot))
		return;

	DistributedSnapshotWithLocalMapping *mapping =
		&(snapshot->distribSnapshotWithLocalMapping);

	char message[MESSAGE_LEN];
	snprintf(message, MESSAGE_LEN, "%s Distributed snapshot info: "
			 "xminAllDistributedSnapshots=%d, distribSnapshotId=%d"
			 ", xmin=%d, xmax=%d, count=%d",
			 prefix,
			 mapping->header.xminAllDistributedSnapshots,
			 mapping->header.distribSnapshotId,
			 mapping->header.xmin,
			 mapping->header.xmax,
			 mapping->header.count);

	snprintf(message, MESSAGE_LEN, "%s, In progress array: {",
			 message);

	for (int no = 0; no < mapping->header.count; no++)
	{
		if (no != 0)
			snprintf(message, MESSAGE_LEN, "%s, (%d,%d)",
					 message, mapping->inProgressEntryArray[no].distribXid,
					 mapping->inProgressEntryArray[no].localXid);
		else
			snprintf(message, MESSAGE_LEN, "%s (%d,%d)",
					 message, mapping->inProgressEntryArray[no].distribXid,
					 mapping->inProgressEntryArray[no].localXid);
	}

	elog(LOG, "%s}", message);
}

struct mpp_xid_map_entry {
	pid_t				pid;
	TransactionId		global;
	TransactionId		local;
};

struct mpp_xid_map {
	int			size;
	int			cur;
	struct mpp_xid_map_entry map[1];
};

/*
 * mpp_global_xid_map
 */
Datum
mpp_global_xid_map(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	struct mpp_xid_map *ctx;
	bool	nulls[3];
	int			i, j;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		MemoryContext oldcontext;
		volatile SharedSnapshotStruct *arrayP = sharedSnapshotArray;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/*
		 * switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		/* this had better match pg_locks view in system_views.sql */
		tupdesc = CreateTemplateTupleDesc(3, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "pid",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "localxid",
						   XIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "globalxid",
						   XIDOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		/* figure out how many slots we need */

		ctx = (struct mpp_xid_map *)palloc0(sizeof(struct mpp_xid_map) +
											arrayP->maxSlots * sizeof(struct mpp_xid_map_entry));

		j = 0;
		LWLockAcquire(SharedSnapshotLock, LW_EXCLUSIVE);
		for (i = 0; i < arrayP->maxSlots; i++)
		{
			SharedSnapshotSlot *testSlot = &arrayP->slots[i];

			if (testSlot->slotid != -1)
			{
				ctx->map[j].pid = testSlot->pid;
				ctx->map[j].local = testSlot->xid;
				ctx->map[j].global = testSlot->QDxid;
				j++;
			}
		}
		LWLockRelease(SharedSnapshotLock);
		ctx->size = j;

		funcctx->user_fctx = (void *)ctx;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	ctx = (struct mpp_xid_map *)funcctx->user_fctx;

	MemSet(nulls, false, sizeof(nulls));
	while (ctx->cur < ctx->size)
	{
		Datum		values[3];

		HeapTuple	tuple;
		Datum		result;

		/*
		 * Form tuple with appropriate data.
		 */
		MemSet(values, 0, sizeof(values));

		values[0] = Int32GetDatum(ctx->map[ctx->cur].pid);
		values[1] = TransactionIdGetDatum(ctx->map[ctx->cur].local);
		values[2] = TransactionIdGetDatum(ctx->map[ctx->cur].global);

		ctx->cur++;

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);
		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
}

/*
 * XidInMVCCSnapshot
 *		Is the given XID still-in-progress according to the distributed
 *      and local snapshots?
 */
static bool
XidInMVCCSnapshot(TransactionId xid, Snapshot snapshot, bool isXmax,
				  bool distributedSnapshotIgnore, bool *setDistributedSnapshotIgnore)
{
	Assert (setDistributedSnapshotIgnore != NULL);
	*setDistributedSnapshotIgnore = false;

	/*
	 * If we have a distributed snapshot, it takes precedence over the local
	 * snapshot since it covers the correct past view of in-progress distributed
	 * transactions and also the correct future view of in-progress distributed
	 * transactions that may yet arrive.
	 */
	if (snapshot->haveDistribSnapshot && !distributedSnapshotIgnore)
	{
		DistributedSnapshotCommitted	distributedSnapshotCommitted;

		/*
		 * First, check if this committed transaction is a distributed committed
		 * transaction and should be evaluated against the distributed snapshot
		 * instead.
		 */
		distributedSnapshotCommitted =
			DistributedSnapshotWithLocalMapping_CommittedTest(
				&snapshot->distribSnapshotWithLocalMapping,
				xid,
				isXmax);

		switch (distributedSnapshotCommitted)
		{
			case DISTRIBUTEDSNAPSHOT_COMMITTED_INPROGRESS:
				return true;

			case DISTRIBUTEDSNAPSHOT_COMMITTED_VISIBLE:
				return false;

			case DISTRIBUTEDSNAPSHOT_COMMITTED_IGNORE:
				/*
				 * We can safely skip both of these in the future for distributed
				 * snapshots.
				 */
				*setDistributedSnapshotIgnore = true;
				break;

			default:
				elog(FATAL, "Unrecognized distributed committed test result: %d",
					 (int) distributedSnapshotCommitted);
				break;
		}
	}

	return XidInMVCCSnapshot_Local(xid, snapshot, isXmax);
}

/*
 * XidInMVCCSnapshot_Local
 *		Is the given XID still-in-progress according to the local snapshot?
 *
 * Note: GetSnapshotData never stores either top xid or subxids of our own
 * backend into a snapshot, so these xids will not be reported as "running"
 * by this function.  This is OK for current uses, because we actually only
 * apply this for known-committed XIDs.
 */
static bool
XidInMVCCSnapshot_Local(TransactionId xid, Snapshot snapshot, bool isXmax)
{
	uint32		i;

	/*
	 * Make a quick range check to eliminate most XIDs without looking at the
	 * xip arrays.	Note that this is OK even if we convert a subxact XID to
	 * its parent below, because a subxact with XID < xmin has surely also got
	 * a parent with XID < xmin, while one with XID >= xmax must belong to a
	 * parent that was not yet committed at the time of this snapshot.
	 */

	/* Any xid < xmin is not in-progress */
	if (TransactionIdPrecedes(xid, snapshot->xmin))
		return false;
	/* Any xid >= xmax is in-progress */
	if (TransactionIdFollowsOrEquals(xid, snapshot->xmax))
		return true;

	/*
	 * If the snapshot contains full subxact data, the fastest way to check
	 * things is just to compare the given XID against both subxact XIDs and
	 * top-level XIDs.	If the snapshot overflowed, we have to use pg_subtrans
	 * to convert a subxact XID to its parent XID, but then we need only look
	 * at top-level XIDs not subxacts.
	 */
	if (snapshot->subxcnt >= 0)
	{
		/* full data, so search subxip */
		int32		j;

		for (j = 0; j < snapshot->subxcnt; j++)
		{
			if (TransactionIdEquals(xid, snapshot->subxip[j]))
				return true;
		}

		/* not there, fall through to search xip[] */
	}
	else
	{
		/* overflowed, so convert xid to top-level */
		xid = SubTransGetTopmostTransaction(xid);

		/*
		 * If xid was indeed a subxact, we might now have an xid < xmin, so
		 * recheck to avoid an array scan.	No point in rechecking xmax.
		 */
		if (TransactionIdPrecedes(xid, snapshot->xmin))
			return false;
	}

	for (i = 0; i < snapshot->xcnt; i++)
	{
		if (TransactionIdEquals(xid, snapshot->xip[i]))
			return true;
	}

	return false;
}

static char *TupleTransactionStatus_Name(TupleTransactionStatus status)
{
	switch (status)
	{
	case TupleTransactionStatus_None: 				return "None";
	case TupleTransactionStatus_Frozen: 			return "Frozen";
	case TupleTransactionStatus_HintCommitted: 		return "Hint-Committed";
	case TupleTransactionStatus_HintAborted: 		return "Hint-Aborted";
	case TupleTransactionStatus_CLogInProgress: 	return "CLog-In-Progress";
	case TupleTransactionStatus_CLogCommitted: 		return "CLog-Committed";
	case TupleTransactionStatus_CLogAborted:		return "CLog-Aborted";
	case TupleTransactionStatus_CLogSubCommitted:	return "CLog-Sub-Committed";
	default:
		return "Unknown";
	}
}

static char *TupleVisibilityStatus_Name(TupleVisibilityStatus status)
{
	switch (status)
	{
	case TupleVisibilityStatus_Unknown: 	return "Unknown";
	case TupleVisibilityStatus_InProgress: 	return "In-Progress";
	case TupleVisibilityStatus_Aborted: 	return "Aborted";
	case TupleVisibilityStatus_Past: 		return "Past";
	case TupleVisibilityStatus_Now: 		return "Now";
	default:
		return "Unknown";
	}
}

static TupleTransactionStatus GetTupleVisibilityCLogStatus(TransactionId xid)
{
	XidStatus xidStatus;
	XLogRecPtr lsn;

	xidStatus = TransactionIdGetStatus(xid, &lsn);
	switch (xidStatus)
	{
	case TRANSACTION_STATUS_IN_PROGRESS:	return TupleTransactionStatus_CLogInProgress;
	case TRANSACTION_STATUS_COMMITTED:		return TupleTransactionStatus_CLogCommitted;
	case TRANSACTION_STATUS_ABORTED:		return TupleTransactionStatus_CLogAborted;
	case TRANSACTION_STATUS_SUB_COMMITTED:	return TupleTransactionStatus_CLogSubCommitted;
	default:
		// Never gets here.  XidStatus is only 2-bits.
		return TupleTransactionStatus_None;
	}
}

void GetTupleVisibilitySummary(
	HeapTuple				tuple,
	TupleVisibilitySummary	*tupleVisibilitySummary)
{
	tupleVisibilitySummary->tid = tuple->t_self;
	tupleVisibilitySummary->infomask = tuple->t_data->t_infomask;
	tupleVisibilitySummary->infomask2 = tuple->t_data->t_infomask2;
	tupleVisibilitySummary->updateTid = tuple->t_data->t_ctid;

	tupleVisibilitySummary->xmin = HeapTupleHeaderGetXmin(tuple->t_data);
	if (!TransactionIdIsNormal(tupleVisibilitySummary->xmin))
	{
		if (tupleVisibilitySummary->xmin == FrozenTransactionId)
		{
			tupleVisibilitySummary->xminStatus = TupleTransactionStatus_Frozen;
		}
		else
		{
			tupleVisibilitySummary->xminStatus = TupleTransactionStatus_None;
		}
	}
	else if (tuple->t_data->t_infomask & HEAP_XMIN_COMMITTED)
	{
		tupleVisibilitySummary->xminStatus = TupleTransactionStatus_HintCommitted;
	}
	else if (tuple->t_data->t_infomask & HEAP_XMIN_INVALID)
	{
		tupleVisibilitySummary->xminStatus = TupleTransactionStatus_HintAborted;
	}
	else
	{
		tupleVisibilitySummary->xminStatus =
					GetTupleVisibilityCLogStatus(tupleVisibilitySummary->xmin);
	}
	tupleVisibilitySummary->xmax = HeapTupleHeaderGetXmax(tuple->t_data);
	if (!TransactionIdIsNormal(tupleVisibilitySummary->xmax))
	{
		if (tupleVisibilitySummary->xmax == FrozenTransactionId)
		{
			tupleVisibilitySummary->xmaxStatus = TupleTransactionStatus_Frozen;
		}
		else
		{
			tupleVisibilitySummary->xmaxStatus = TupleTransactionStatus_None;
		}
	}
	else if (tuple->t_data->t_infomask & HEAP_XMAX_COMMITTED)
	{
		tupleVisibilitySummary->xmaxStatus = TupleTransactionStatus_HintCommitted;
	}
	else if (tuple->t_data->t_infomask & HEAP_XMAX_INVALID)
	{
		tupleVisibilitySummary->xmaxStatus = TupleTransactionStatus_HintAborted;
	}
	else
	{
		tupleVisibilitySummary->xmaxStatus =
					GetTupleVisibilityCLogStatus(tupleVisibilitySummary->xmax);
	}

	tupleVisibilitySummary->cid =
					HeapTupleHeaderGetRawCommandId(tuple->t_data);

	/*
	 * Evaluate xmin and xmax status to produce overall visibility.
	 *
	 * UNDONE: Too simplistic?
	 */
	switch (tupleVisibilitySummary->xminStatus)
	{
	case TupleTransactionStatus_None:
	case TupleTransactionStatus_CLogSubCommitted:
		tupleVisibilitySummary->visibilityStatus = TupleVisibilityStatus_Unknown;
		break;

	case TupleTransactionStatus_CLogInProgress:
		tupleVisibilitySummary->visibilityStatus = TupleVisibilityStatus_InProgress;
		break;

	case TupleTransactionStatus_HintAborted:
	case TupleTransactionStatus_CLogAborted:
		tupleVisibilitySummary->visibilityStatus = TupleVisibilityStatus_Aborted;
		break;

	case TupleTransactionStatus_Frozen:
	case TupleTransactionStatus_HintCommitted:
	case TupleTransactionStatus_CLogCommitted:
		{
			switch (tupleVisibilitySummary->xmaxStatus)
			{
			case TupleTransactionStatus_None:
			case TupleTransactionStatus_Frozen:
			case TupleTransactionStatus_CLogInProgress:
			case TupleTransactionStatus_HintAborted:
			case TupleTransactionStatus_CLogAborted:
				tupleVisibilitySummary->visibilityStatus = TupleVisibilityStatus_Now;
				break;

			case TupleTransactionStatus_CLogSubCommitted:
				tupleVisibilitySummary->visibilityStatus = TupleVisibilityStatus_Unknown;
				break;

			case TupleTransactionStatus_HintCommitted:
			case TupleTransactionStatus_CLogCommitted:
				tupleVisibilitySummary->visibilityStatus = TupleVisibilityStatus_Past;
				break;

			default:
				elog(ERROR, "Unrecognized tuple transaction status: %d",
					 (int) tupleVisibilitySummary->xmaxStatus);
				tupleVisibilitySummary->visibilityStatus = TupleVisibilityStatus_Unknown;
				break;
			}
		}
		break;

	default:
		elog(ERROR, "Unrecognized tuple transaction status: %d",
			 (int) tupleVisibilitySummary->xminStatus);
		tupleVisibilitySummary->visibilityStatus = TupleVisibilityStatus_Unknown;
		break;
	}
}

static char *GetTupleVisibilityDistribId(
	TransactionId 				xid,
	TupleTransactionStatus      status)
{
	DistributedTransactionTimeStamp distribTimeStamp;
	DistributedTransactionId		distribXid;

	switch (status)
	{
	case TupleTransactionStatus_None:
	case TupleTransactionStatus_Frozen:
	case TupleTransactionStatus_CLogInProgress:
	case TupleTransactionStatus_HintAborted:
	case TupleTransactionStatus_CLogAborted:
	case TupleTransactionStatus_CLogSubCommitted:
		return NULL;

	case TupleTransactionStatus_HintCommitted:
	case TupleTransactionStatus_CLogCommitted:
		if (DistributedLog_CommittedCheck(
										xid,
										&distribTimeStamp,
										&distribXid))
		{
			char	*distribId;

			distribId = palloc(TMGIDSIZE);
			sprintf(distribId, "%u-%.10u",
					distribTimeStamp, distribXid);
			return distribId;
		}
		else
		{
			return pstrdup("(info not avail)");
		}
		break;

	default:
		elog(ERROR, "Unrecognized tuple transaction status: %d",
			 (int) status);
		return NULL;
	}
}

static void TupleVisibilityAddFlagName(
	StringInfoData *buf,

	int16			rawFlag,

	char			*flagName,

	bool			*atLeastOne)
{
	if (rawFlag != 0)
	{
		if (*atLeastOne)
		{
			appendStringInfo(buf, ", ");
		}
		appendStringInfo(buf, "%s", flagName);
		*atLeastOne = true;
	}
}

static char *GetTupleVisibilityInfoMaskSet(
	int16 				infomask,

	int16				infomask2)
{
	StringInfoData buf;

	bool atLeastOne;

	initStringInfo(&buf);
	appendStringInfo(&buf, "{");
	atLeastOne = false;

	TupleVisibilityAddFlagName(&buf, infomask & HEAP_COMBOCID, "HEAP_COMBOCID", &atLeastOne);
	TupleVisibilityAddFlagName(&buf, infomask & HEAP_XMAX_EXCL_LOCK, "HEAP_XMAX_EXCL_LOCK", &atLeastOne);
	TupleVisibilityAddFlagName(&buf, infomask & HEAP_XMAX_SHARED_LOCK, "HEAP_XMAX_SHARED_LOCK", &atLeastOne);
	TupleVisibilityAddFlagName(&buf, infomask & HEAP_XMIN_COMMITTED, "HEAP_XMIN_COMMITTED", &atLeastOne);
	TupleVisibilityAddFlagName(&buf, infomask & HEAP_XMIN_INVALID, "HEAP_XMIN_INVALID", &atLeastOne);
	TupleVisibilityAddFlagName(&buf, infomask & HEAP_XMAX_COMMITTED, "HEAP_XMAX_COMMITTED", &atLeastOne);
	TupleVisibilityAddFlagName(&buf, infomask & HEAP_XMAX_INVALID, "HEAP_XMAX_INVALID", &atLeastOne);
	TupleVisibilityAddFlagName(&buf, infomask & HEAP_XMAX_IS_MULTI, "HEAP_XMAX_IS_MULTI", &atLeastOne);
	TupleVisibilityAddFlagName(&buf, infomask & HEAP_UPDATED, "HEAP_UPDATED", &atLeastOne);
	TupleVisibilityAddFlagName(&buf, infomask & HEAP_MOVED_OFF, "HEAP_MOVED_OFF", &atLeastOne);
	TupleVisibilityAddFlagName(&buf, infomask & HEAP_MOVED_IN, "HEAP_MOVED_IN", &atLeastOne);

	TupleVisibilityAddFlagName(&buf, infomask2 & HEAP_XMIN_DISTRIBUTED_SNAPSHOT_IGNORE, "HEAP_XMIN_DISTRIBUTED_SNAPSHOT_IGNORE", &atLeastOne);
	TupleVisibilityAddFlagName(&buf, infomask2 & HEAP_XMAX_DISTRIBUTED_SNAPSHOT_IGNORE, "HEAP_XMAX_DISTRIBUTED_SNAPSHOT_IGNORE", &atLeastOne);

	appendStringInfo(&buf, "}");
	return buf.data;
}

// 0  gp_tid                    			TIDOID
// 1  gp_xmin                  			INT4OID
// 2  gp_xmin_status        			TEXTOID
// 3  gp_xmin_commit_distrib_id		TEXTOID
// 4  gp_xmax		          		INT4OID
// 5  gp_xmax_status       			TEXTOID
// 6  gp_xmax_distrib_id    			TEXTOID
// 7  gp_command_id	    			INT4OID
// 8  gp_infomask    	   			TEXTOID
// 9  gp_update_tid         			TIDOID
// 10 gp_visibility             			TEXTOID

void GetTupleVisibilitySummaryDatums(
	Datum		*values,
	bool		*nulls,
	TupleVisibilitySummary	*tupleVisibilitySummary)
{
	char *xminDistribId;
	char *xmaxDistribId;
	char *infoMaskSet;

	values[0] = ItemPointerGetDatum(&tupleVisibilitySummary->tid);
	values[1] = Int32GetDatum((int32)tupleVisibilitySummary->xmin);
	values[2] =
		DirectFunctionCall1(textin,
							CStringGetDatum(
								TupleTransactionStatus_Name(
											tupleVisibilitySummary->xminStatus)));
	xminDistribId = GetTupleVisibilityDistribId(
									tupleVisibilitySummary->xmin,
									tupleVisibilitySummary->xminStatus);
	if (xminDistribId != NULL)
	{
		values[3] =
			DirectFunctionCall1(textin,
								CStringGetDatum(
										xminDistribId));
		pfree(xminDistribId);
	}
	else
	{
		nulls[3] = true;
	}
	values[4] = Int32GetDatum((int32)tupleVisibilitySummary->xmax);
	values[5] =
		DirectFunctionCall1(textin,
							CStringGetDatum(
								TupleTransactionStatus_Name(
											tupleVisibilitySummary->xmaxStatus)));
	xmaxDistribId = GetTupleVisibilityDistribId(
									tupleVisibilitySummary->xmax,
									tupleVisibilitySummary->xmaxStatus);
	if (xmaxDistribId != NULL)
	{
		values[6] =
			DirectFunctionCall1(textin,
								CStringGetDatum(
										xmaxDistribId));
		pfree(xmaxDistribId);
	}
	else
	{
		nulls[6] = true;
	}
	values[7] = Int32GetDatum((int32)tupleVisibilitySummary->cid);
	infoMaskSet = GetTupleVisibilityInfoMaskSet(
									tupleVisibilitySummary->infomask,
									tupleVisibilitySummary->infomask2);
	values[8] =
		DirectFunctionCall1(textin,
							CStringGetDatum(
									infoMaskSet));
	pfree(infoMaskSet);
	values[9] = ItemPointerGetDatum(&tupleVisibilitySummary->updateTid);
	values[10] =
		DirectFunctionCall1(textin,
							CStringGetDatum(
								TupleVisibilityStatus_Name(
											tupleVisibilitySummary->visibilityStatus)));
}

char *GetTupleVisibilitySummaryString(
	TupleVisibilitySummary	*tupleVisibilitySummary)
{
	StringInfoData buf;

	char *xminDistribId;
	char *xmaxDistribId;
	char *infoMaskSet;

	initStringInfo(&buf);
	appendStringInfo(&buf, "tid %s",
					 ItemPointerToString(&tupleVisibilitySummary->tid));
	appendStringInfo(&buf, ", xmin %u",
					 tupleVisibilitySummary->xmin);
	appendStringInfo(&buf, ", xmin_status '%s'",
					 TupleTransactionStatus_Name(
							tupleVisibilitySummary->xminStatus));

	xminDistribId = GetTupleVisibilityDistribId(
									tupleVisibilitySummary->xmin,
									tupleVisibilitySummary->xminStatus);
	if (xminDistribId != NULL)
	{
		appendStringInfo(&buf, ", xmin_commit_distrib_id '%s'",
						 xminDistribId);
		pfree(xminDistribId);
	}
	appendStringInfo(&buf, ", xmax %u",
					 tupleVisibilitySummary->xmax);
	appendStringInfo(&buf, ", xmax_status '%s'",
					 TupleTransactionStatus_Name(
							tupleVisibilitySummary->xmaxStatus));

	xmaxDistribId = GetTupleVisibilityDistribId(
									tupleVisibilitySummary->xmax,
									tupleVisibilitySummary->xmaxStatus);
	if (xmaxDistribId != NULL)
	{
		appendStringInfo(&buf, ", xmax_commit_distrib_id '%s'",
						 xmaxDistribId);
		pfree(xmaxDistribId);
	}
	appendStringInfo(&buf, ", command_id %u",
					 tupleVisibilitySummary->cid);
	infoMaskSet = GetTupleVisibilityInfoMaskSet(
									tupleVisibilitySummary->infomask,
									tupleVisibilitySummary->infomask2);
	appendStringInfo(&buf, ", infomask '%s'",
					 infoMaskSet);
	pfree(infoMaskSet);
	appendStringInfo(&buf, ", update_tid %s",
					 ItemPointerToString(&tupleVisibilitySummary->updateTid));
	appendStringInfo(&buf, ", visibility '%s'",
					 TupleVisibilityStatus_Name(
											tupleVisibilitySummary->visibilityStatus));

	return buf.data;
}
