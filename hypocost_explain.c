#include "postgres.h"
#include "tcop/tcopprot.h"
#include "hypocost.h"

bool hypocost_in_explain = false;
bool hypocost_in_explain_analyze = false;


void
hypocost_explain(Query *query, int cursorOptions,
				 IntoClause *into, ExplainState *es,
				 const char *queryString, ParamListInfo params,
				 QueryEnvironment *queryEnv)
{
	/* planner will not cope with utility statements */
	if (query->commandType == CMD_UTILITY)
	{
		ExplainOneUtility(query->utilityStmt, into, es, queryString, params, queryEnv);
		return;
	}

	// Start...
        hypocost_in_explain = hypocost_enable;
        PG_TRY();
	{
		PlannedStmt *plan;
		instr_time	planstart, planduration;
		BufferUsage bufusage_start, bufusage;
		if (es->buffers)
			bufusage_start = pgBufferUsage;
		INSTR_TIME_SET_CURRENT(planstart);

		/* plan the query */
		plan = pg_plan_query(query, queryString, cursorOptions, params);

		INSTR_TIME_SET_CURRENT(planduration);
		INSTR_TIME_SUBTRACT(planduration, planstart);

		/* calc differences of buffer counters. */
		if (es->buffers)
		{
			memset(&bufusage, 0, sizeof(BufferUsage));
			BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &bufusage_start);
		}

		if (hypocost_in_explain_analyze)
		{
			// Turn analyze back on...
			es->analyze = true;
			es->summary = true;
		}

		/* run it (if needed) and produce output */
		ExplainOnePlan(plan, into, es, queryString, params, queryEnv, &planduration, (es->buffers ? &bufusage : NULL));
	}
        PG_FINALLY();
        {
                hypocost_in_explain = false;
        }
        PG_END_TRY();
}
