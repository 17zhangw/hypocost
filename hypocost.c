#include "postgres.h"
#include "fmgr.h"
#include "utils/guc.h"
#include "float.h"
#include "tcop/utility.h"

#include "hypocost.h"

PG_MODULE_MAGIC;

void _PG_init(void);

bool hypocost_enable = false;
bool hypocost_substitute = false;
bool hypocost_inject_analyze = false;
double hypocost_seq_page_cost = 1.0;
double hypocost_random_page_cost = 4.0;

static ProcessUtility_hook_type prev_utility_hook = NULL;

static void
hypocost_utility_hook(
	PlannedStmt *pstmt,
	const char *queryString,
	bool readOnlyTree,
	ProcessUtilityContext context,
	ParamListInfo params,
	QueryEnvironment *queryEnv,
	DestReceiver *dest,
	QueryCompletion *qc)
{
        Node* parsetree = ((PlannedStmt *) pstmt)->utilityStmt;
        if (parsetree != NULL && hypocost_inject_analyze)
	{
		switch (nodeTag(parsetree))
		{
			case T_ExplainStmt:
			{
				ListCell   *lc;

                                foreach(lc, ((ExplainStmt *) parsetree)->options)
                                {
                                        DefElem    *opt = (DefElem *) lfirst(lc);

					if (strcmp(opt->defname, "analyze") == 0)
					{
						((ExplainStmt*)parsetree)->options = foreach_delete_current(((ExplainStmt*)parsetree)->options, lc);
						hypocost_in_explain_analyze = true;
					}
				}
			}
			break;
                default:
			break;
		}
        }

	PG_TRY();
	{
		if (prev_utility_hook)
		{
			prev_utility_hook(
				pstmt,
				queryString,
				readOnlyTree,
				context,
				params,
				queryEnv,
				dest,
				qc);
		}
		else
		{
			standard_ProcessUtility(
				pstmt,
				queryString,
				readOnlyTree,
				context,
				params,
				queryEnv,
				dest,
				qc);
		}
	}
	PG_FINALLY();
	{
		hypocost_in_explain_analyze = false;
	}
	PG_END_TRY();
}

void _PG_init(void) {
        DefineCustomBoolVariable("hypocost.enable", "Enable Hypocost.", NULL, &hypocost_enable, false, PGC_SUSET, 0, NULL, NULL, NULL);
        DefineCustomBoolVariable("hypocost.inject_analyze", "Attempt to inject into analyze.", NULL, &hypocost_inject_analyze, false, PGC_SUSET, 0, NULL, NULL, NULL);
        DefineCustomBoolVariable("hypocost.substitute", "Attempt to substitute.", NULL, &hypocost_substitute, false, PGC_SUSET, 0, NULL, NULL, NULL);
        DefineCustomRealVariable(
                "hypocost.seq_page_cost", 
                "Hypocost Seq Page Cost",
                "Hypocost Seq Page Cost",
                &hypocost_seq_page_cost,
                1.0,
                0,
                DBL_MAX,
                PGC_SUSET,
                0,
                NULL,
                NULL,
                NULL
        );
        DefineCustomRealVariable(
                "hypocost.random_page_cost", 
                "Hypocost Random Page Cost",
                "Hypocost Random Page Cost",
                &hypocost_random_page_cost,
                4.0,
                0,
                DBL_MAX,
                PGC_SUSET,
                0,
                NULL,
                NULL,
                NULL
        );


        MarkGUCPrefixReserved("hypocost");

		if (planner_hook != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("hypocost must be initialized first onto the planner.")));

		if (ExplainOneQuery_hook != NULL)
			ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("hypocost must be initialized first on explain")));

		prev_utility_hook = ProcessUtility_hook;
		ProcessUtility_hook = hypocost_utility_hook;

		planner_hook = hypocost_planner;
		ExplainOneQuery_hook = hypocost_explain;
		planner_cost_scribble_hook = hypocost_scribble;
		planner_pick_altsubplan_hook = hypocost_pick_altsubplan;
}
