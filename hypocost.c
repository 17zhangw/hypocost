#include "postgres.h"
#include "fmgr.h"
#include "utils/guc.h"
#include "float.h"

#include "hypocost.h"

PG_MODULE_MAGIC;

void _PG_init(void);

bool hypocost_enable = false;
double hypocost_seq_page_cost = 1.0;
double hypocost_random_page_cost = 4.0;
ExplainOneQuery_hook_type prev_explain_hook = NULL;

void _PG_init(void) {
        DefineCustomBoolVariable("hypocost.enable", "Enable Hypocost.", NULL, &hypocost_enable, false, PGC_SUSET, 0, NULL, NULL, NULL);
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

		planner_hook = hypocost_planner;
        prev_explain_hook = ExplainOneQuery_hook;
		ExplainOneQuery_hook = hypocost_explain;
		planner_cost_scribble_hook = hypocost_scribble;
		planner_pick_altsubplan_hook = hypocost_pick_altsubplan;
}
