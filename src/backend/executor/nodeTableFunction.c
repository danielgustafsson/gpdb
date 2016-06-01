/*-------------------------------------------------------------------------
 *
 * nodeTableFunction.c
 *	 Support routines for scans of enhanced table functions.
 *
 * DESCRIPTION
 *
 *   This code is distinct from ExecFunctionScan due to the nature of
 *   the plans.  A plain table function will be called without an input
 *   subquery, whereas the enhanced table function framework allows 
 *   table functions operating over table input.
 *
 *  Normal Table Function            Enhanced Table Function
 *
 *         (out)                              (out)
 *           |                                  |
 *     (FunctionScan)                  (TableFunctionScan)
 *                                              |
 *                                        (SubqueryScan)
 *
 * INTERFACE ROUTINES
 * 	 ExecTableFunctionScan			sequentially scans a relation.
 *	 ExecTableFunctionNext			retrieve next tuple in sequential order.
 *	 ExecInitTableFunctionScan		creates and initializes a externalscan node.
 *	 ExecEndTableFunctionScan		releases any storage allocated.
 *	 ExecStopTableFunctionScan		closes external resources before EOD.
 *	 ExecTableFunctionReScan		rescans the relation
 *
 * Copyright (c) 2011, EMC
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"
#include "tablefuncapi.h"

#include "cdb/cdbvars.h"
#include "executor/executor.h"
#include "executor/nodeTableFunction.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"


static void setupFunctionArguments(TableFunctionState *node);
static TupleTableSlot *TableFunctionNext(TableFunctionState *node);
static gpmon_packet_t *GpmonPktFromTableFunctionState(TableFunctionState *node);
static void initGpmonPktForTableFunction(Plan *planNode,
										 gpmon_packet_t *gpmon_pkt, 
										 EState *estate);

/* Private structure forward declared in tablefuncapi.h */
typedef struct AnyTableData
{
	ExprContext			*econtext;
	PlanState			*subplan;     /* subplan node */
	TupleDesc            subdesc;     /* tuple descriptor of subplan */
	JunkFilter          *junkfilter;  /* for projection of subplan tuple */
} AnyTableData;

/*
 * setupFunctionArguments
 */
static void
setupFunctionArguments(TableFunctionState *node)
{
	ExprContext	*econtext = node->ss.ps.ps_ExprContext;
	int			 count	  = 0;
	int			 i		  = 0;
	ListCell	*arg	  = NULL;
	bool		 argDone;

	/* Evaluate the static function args */
	argDone = ExecEvalFuncArgs(&node->fcinfo, 
							   node->fcache->args, 
							   econtext);

	/*
	 * We don't allow sets in the arguments of the table function (except for
	 * specific anytable values generated by TableValueExpressions).
	 */
	if (argDone != ExprSingleResult)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot "
						"accept a set")));
	}

	/*
	 * Identify the AnyTable Arguement of the function and swap it with the
	 * AnyTableData for our subplan. 
	 */
	foreach(arg, node->fcache->args)
	{
		ExprState *argstate = (ExprState *) lfirst(arg);

		if (IsA(argstate->expr, TableValueExpr))
		{
			node->fcinfo.arg[i]		= AnyTableGetDatum(node->inputscan);
			node->fcinfo.argnull[i] = false;
			count++;
		}
		i++;
	}

	/* 
	 * Currently we don't allow table functions with more than one 
	 * table value expression arguments, and if they don't have at
	 * least one they will be planned as nodeFunctionScan instead 
	 * of nodeTableFunctionScan.  Therefore we should have found
	 * exactly 1 TableValueExpr above.
	 */
	if (count != 1)
	{
		elog(ERROR, 
			 "table functions over multiple TABLE value expressions "
			 "not yet supported");  /* not feasible */
	}
}

/*
 * TableFunctionNext - ExecScan callback function for table funciton scans 
 */
static TupleTableSlot *
TableFunctionNext(TableFunctionState *node)
{
	MemoryContext        oldcontext  = NULL;
	TupleTableSlot		*slot        = NULL;
	ExprContext			*econtext	 = node->ss.ps.ps_ExprContext;
	FuncExprState       *fcache		 = node->fcache; 
	bool                 returns_set = fcache->func.fn_retset;
	HeapTuple            tuple       = NULL;
	TupleDesc            resultdesc  = node->resultdesc;
	Datum                user_result;

	/* Clean up any per-tuple memory */
	ResetExprContext(econtext);

	/*
	 * If all results have been returned by the callback function then
	 * we are done. 
	 */
	if (node->rsinfo.isDone == ExprEndResult)
		return slot;  /* empty slot */

	/* Invoke the user supplied function */
	node->fcinfo.isnull = false;
	oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
	user_result = FunctionCallInvoke(&node->fcinfo);
	MemoryContextSwitchTo(oldcontext);
	if (node->rsinfo.returnMode != SFRM_ValuePerCall)
	{
		/* FIXME: should support both protocols */
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED),
				 errmsg("table functions must use SFRM_ValuePerCall protocol")));
	}
	if (node->rsinfo.isDone == ExprEndResult)
		return slot;  /* empty slot */

	/* Mark this the last value if the func doesn't return a set */
	if (!returns_set || node->rsinfo.isDone == ExprSingleResult)
		node->rsinfo.isDone = ExprEndResult;

	/* This would only error if the user violated the SRF calling convensions */
	if (returns_set && node->fcinfo.isnull)
	{
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("function returning set of rows cannot return null value")));
	}

	/* Convert the Datum into tuple and store it into the scan slot */
	if (node->is_rowtype)
	{
		HeapTupleHeader  th;

		if (node->fcinfo.isnull)
		{
			int			i;
			Datum		values[MaxTupleAttributeNumber];
			bool		nulls[MaxTupleAttributeNumber];

			Insist(!returns_set);  /* checked above */
			Insist(resultdesc->natts <= MaxTupleAttributeNumber);
			for (i = 0; i < resultdesc->natts; i++)
				nulls[i] = true;

			/* 
			 * If we get a clean solution to the tuple allocation below we
			 * can use it here as well.  This is less an issue because there
			 * is only a single tuple in this case, so the overhead of a
			 * single palloc is not a big deal.
			 */
			oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
			tuple = heap_form_tuple(resultdesc, values, nulls);
			MemoryContextSwitchTo(oldcontext);
		}
		else
		{

			/* Convert returned HeapTupleHeader into a HeapTuple */
			th	  = DatumGetHeapTupleHeader(user_result);
			tuple = &node->tuple;

			ItemPointerSetInvalid(&(tuple->t_self));
			tuple->t_len  = HeapTupleHeaderGetDatumLength(th);
			tuple->t_data = th;

			/* Double check that this tuple is of the expected form */
			if (resultdesc->tdtypeid != HeapTupleHeaderGetTypeId(th))
			{
				ereport(ERROR, 
						(errcode(ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED),
						 errmsg("invalid tuple returned from table function"),
						 errdetail("Returned tuple does not match output "
								   "tuple descriptor.")));
			}
		}
	}
	else
	{
		/* 
		 * TODO: This will allocate memory for each tuple, we should be able 
		 * to get away with fewer pallocs.
		 */
		oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
		tuple = heap_form_tuple(resultdesc, 
								&user_result, 
								&node->fcinfo.isnull);
		MemoryContextSwitchTo(oldcontext);
	}

	/* 
	 * Store the tuple into the scan slot.
	 *
	 * Note: Tuple should be allocated in the per-row memory context, so they
	 * will be freed automatically when the context is freed, we cannot free
	 * them again here.
	 */
	Assert(tuple);
	slot = ExecStoreHeapTuple(tuple, 
							  node->ss.ss_ScanTupleSlot, 
							  InvalidBuffer, 
							  false /* shouldFree */);
	Assert(!TupIsNull(slot));

	node->ss.ss_ScanTupleSlot = slot;

	/* Update gpmon statistics */
	if (!TupIsNull(slot))
	{
		Gpmon_M_Incr_Rows_Out(GpmonPktFromTableFunctionState(node));
		CheckSendPlanStateGpmonPkt(&node->ss.ps);
	}

	return slot;
}

/*
 * ExecTableFunction - wrapper around TableFunctionNext
 */
TupleTableSlot *
ExecTableFunction(TableFunctionState *node)
{
	/* Setup arguments on the first call */
	if (node->is_firstcall)
	{
		setupFunctionArguments(node);
		node->is_firstcall = false;
	}

	return ExecScan(&node->ss, (ExecScanAccessMtd) TableFunctionNext);
}


/*
 * ExecInitTableFunction - Setup the table function executor 
 */
TableFunctionState *
ExecInitTableFunction(TableFunctionScan *node, EState *estate, int eflags)
{
	TableFunctionState	*scanstate;
	PlanState           *subplan;
	RangeTblEntry		*rte;
	Oid					 funcrettype;
	TypeFuncClass		 functypclass;
	FuncExpr            *func;
	ExprContext         *econtext;
	TupleDesc            inputdesc  = NULL;
	TupleDesc			 resultdesc = NULL;

	/* Inner plan is not used, outer plan must be present */
	Assert(innerPlan(node) == NULL);
	Assert(outerPlan(node) != NULL);

	/* Forward scan only */
	Assert(!(eflags & (EXEC_FLAG_MARK | EXEC_FLAG_BACKWARD)));

	/*
	 * Create state structure.
	 */
	scanstate = makeNode(TableFunctionState);
	scanstate->ss.ps.plan  = (Plan *)node;
	scanstate->ss.ps.state = estate;
	scanstate->inputscan   = palloc0(sizeof(AnyTableData));
	scanstate->is_firstcall = true;

	/* Create expression context for the node. */
	ExecAssignExprContext(estate, &scanstate->ss.ps);
	ExecInitResultTupleSlot(estate, &scanstate->ss.ps);
	ExecInitScanTupleSlot(estate, &scanstate->ss);
	econtext = scanstate->ss.ps.ps_ExprContext;
	
	/* Initialize child expressions */
	scanstate->ss.ps.targetlist = (List *)
		ExecInitExpr((Expr *)node->scan.plan.targetlist,
					 (PlanState *)scanstate);
	scanstate->ss.ps.qual = (List *)
		ExecInitExpr((Expr *)node->scan.plan.qual,
					 (PlanState *)scanstate);

	/* Initialize child nodes */
	outerPlanState(scanstate) = ExecInitNode(outerPlan(node), estate, eflags);
	subplan   = outerPlanState(scanstate);
	inputdesc = CreateTupleDescCopy(ExecGetResultType(subplan));

	/* get info about the function */
	rte	 = rt_fetch(node->scan.scanrelid, estate->es_range_table);
	Insist(rte->rtekind == RTE_TABLEFUNCTION);

	/* 
	 * The funcexpr must be a function call.  This check is to verify that
	 * the planner didn't try to perform constant folding or other inlining
	 * on a function invoked as a table function.
	 */
	if (!node->funcexpr || !IsA(node->funcexpr, FuncExpr))
	{
		/* should not be possible */
		elog(ERROR, "table function expression is not a function expression");
	}
	func = (FuncExpr *) node->funcexpr;
	functypclass = get_expr_result_type((Node*) func, &funcrettype, &resultdesc);
	
	switch (functypclass)
	{
		case TYPEFUNC_COMPOSITE:
		{
			/* Composite data type: copy the typcache entry for safety */
			Assert(resultdesc);
			resultdesc = CreateTupleDescCopy(resultdesc);
			scanstate->is_rowtype = true;
			break;
		}

		case TYPEFUNC_RECORD:
		{
			/* Record data type: Construct tuple desc based on rangeTable */
			resultdesc = BuildDescFromLists(node->funccolnames,
											node->funccoltypes,
											node->funccoltypmods);
			scanstate->is_rowtype = true;
			break;
		}

		case TYPEFUNC_SCALAR:
		{
			/* Scalar data type: Construct a tuple descriptor manually */
			char	   *attname = strVal(linitial(node->funccolnames));

			resultdesc = CreateTemplateTupleDesc(1, false);
			TupleDescInitEntry(resultdesc,
							   (AttrNumber) 1,
							   attname,
							   funcrettype,
							   -1,
							   0);
			scanstate->is_rowtype = false;
			break;
		}

		default:
		{
			/* This should not be possible, it should be caught by parser. */
			elog(ERROR, "table function has unsupported return type");
		}
	}

	/*
	 * For RECORD results, make sure a typmod has been assigned.  (The
	 * function should do this for itself, but let's cover things in case it
	 * doesn't.)
	 */
	BlessTupleDesc(resultdesc);
	scanstate->resultdesc = resultdesc;
	ExecAssignScanType(&scanstate->ss, resultdesc);

	/* Other node-specific setup */
	scanstate->fcache = (FuncExprState*)
		ExecInitExpr((Expr *) node->funcexpr, (PlanState *) scanstate);
	Assert(scanstate->fcache && IsA(scanstate->fcache, FuncExprState));

	scanstate->rsinfo.type		   = T_ReturnSetInfo;
	scanstate->rsinfo.econtext	   = econtext;
	scanstate->rsinfo.expectedDesc = resultdesc;
	scanstate->rsinfo.allowedModes = (int) (SFRM_ValuePerCall);
	scanstate->rsinfo.returnMode   = (int) (SFRM_ValuePerCall);
	scanstate->rsinfo.isDone	   = ExprSingleResult;
	scanstate->rsinfo.setResult    = NULL;
	scanstate->rsinfo.setDesc	   = NULL;

	scanstate->userdata = rte->funcuserdata;
	/* Initialize a function cache for the function expression */
	init_fcache(func->funcid, scanstate->fcache, 
				econtext->ecxt_per_query_memory, 
				true);

	/* Initialize the function call info */
	InitFunctionCallInfoData(scanstate->fcinfo,               /* Fcinfo  */
							 &(scanstate->fcache->func),      /* Flinfo  */
							 0,                               /* Nargs   */
							 (Node*) scanstate,               /* Context */
							 (Node*) &(scanstate->rsinfo));   /* ResultInfo */

	/* setup the AnyTable input */
	scanstate->inputscan->econtext = econtext;
	scanstate->inputscan->subplan  = subplan;
	scanstate->inputscan->subdesc  = inputdesc;

	/* Determine projection information for subplan */
	TupleDesc cleanTupType = ExecCleanTypeFromTL(subplan->plan->targetlist, 
						     false /* hasoid */);

	scanstate->inputscan->junkfilter =
		ExecInitJunkFilter(subplan->plan->targetlist, 
						   cleanTupType,
						   NULL  /* slot */);
	BlessTupleDesc(scanstate->inputscan->junkfilter->jf_cleanTupType);

	/* Initialize result tuple type and projection info */
	ExecAssignResultTypeFromTL(&scanstate->ss.ps);
	ExecAssignScanProjectionInfo(&scanstate->ss);

	initGpmonPktForTableFunction((Plan *)node, 
								 &scanstate->ss.ps.gpmon_pkt, estate);
	
	return scanstate;
}

int
ExecCountSlotsTableFunction(TableFunctionScan *node)
{
	return ExecCountSlotsNode(outerPlan(node)) + 2;
}

void
ExecEndTableFunction(TableFunctionState *node)
{
	/* Free the ExprContext */
	ExecFreeExprContext(&node->ss.ps);
	
	/* Clean out the tuple table */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	
	/* End the subplans */
	ExecEndNode(outerPlanState(node));
	
	EndPlanStateGpmonPkt(&node->ss.ps);
}

void
ExecReScanTableFunction(TableFunctionState *node, ExprContext *exprCtxt)
{
	/* TableFunction Planner marks TableFunction nodes as not rescannable */
	elog(ERROR, "invalid rescan of TableFunctionScan");
}


void
initGpmonPktForTableFunction(Plan *planNode, 
							 gpmon_packet_t *gpmon_pkt, 
							 EState *estate)
{
	Assert(planNode != NULL);
	Assert(gpmon_pkt != NULL);
	Assert(IsA(planNode, TableFunctionScan));

	InitPlanNodeGpmonPkt(planNode, gpmon_pkt, estate, PMNT_TableFunctionScan,
						 (int64) planNode->plan_rows, NULL);
}

gpmon_packet_t *
GpmonPktFromTableFunctionState(TableFunctionState *node)
{
	return &node->ss.ps.gpmon_pkt;
}


/* Callback functions exposed to the user */
TupleDesc 
AnyTable_GetTupleDesc(AnyTable t)
{
	if (t == NULL)
	{
		ereport(ERROR, 
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid null value for anytable type")));
	}
	Insist(t->junkfilter && IsA(t->junkfilter, JunkFilter));

	/* Return the projected tuple descriptor */
	return t->junkfilter->jf_cleanTupType;
}

HeapTuple
AnyTable_GetNextTuple(AnyTable t)
{
	MemoryContext oldcontext;

	if (t == NULL)
	{
		ereport(ERROR, 
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid null value for anytable type")));
	}

	/* Fetch the next tuple into the tuple slot */
	oldcontext = MemoryContextSwitchTo(t->econtext->ecxt_per_query_memory);
	t->econtext->ecxt_outertuple = ExecProcNode(t->subplan);
	MemoryContextSwitchTo(oldcontext);
	if (TupIsNull(t->econtext->ecxt_outertuple))
	{
		return (HeapTuple) NULL;
	}

	/* ----------------------------------------
	 * 1) Fetch the tuple from the tuple slot
	 * 2) apply resjunk filtering
	 * 3) copy result into a HeapTuple
	 * ----------------------------------------
	 */
	return ExecRemoveJunk(t->junkfilter, t->econtext->ecxt_outertuple);
}

/*
 * tf_set_userdata_internal
 * This is the entity of TF_SET_USERDATA() API. Sets bytea datum to
 * RangeTblEntry, which is transported to project function via serialized
 * plan tree.
 */
void
tf_set_userdata_internal(FunctionCallInfo fcinfo, bytea *userdata)
{
	if (!fcinfo->context || !IsA(fcinfo->context, RangeTblEntry))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("expected RangeTblEntry node, found %d",
				 fcinfo->context ? nodeTag(fcinfo->context) : 0)));

	/* Make sure it gets detoasted, but packed is allowed */
	((RangeTblEntry *) fcinfo->context)->funcuserdata =
						userdata ? pg_detoast_datum_packed(userdata) : NULL;
}

/*
 * tf_get_userdata_internal
 * This is the entity of TF_GET_USERDATA() API. Extracts userdata from
 * its scan node which was transported via serialized plan tree.
 */
bytea *
tf_get_userdata_internal(FunctionCallInfo fcinfo)
{
	bytea	   *userdata;

	if (!fcinfo->context || !IsA(fcinfo->context, TableFunctionState))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("expected TableFunctionState node, found %d",
				 fcinfo->context ? nodeTag(fcinfo->context) : 0)));

	userdata = ((TableFunctionState *) fcinfo->context)->userdata;
	if (!userdata)
		return NULL;

	/* unpack, just in case */
	return pg_detoast_datum(userdata);
}
