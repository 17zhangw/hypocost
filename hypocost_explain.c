#include "postgres.h"
#include "tcop/tcopprot.h"
#include "hypocost.h"

bool hypocost_in_explain_analyze = false;
struct PartialExplainContext *es_ctx = NULL;


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
        PG_TRY();
	{
		PlannedStmt *plan;
		instr_time	planstart, planduration;
		BufferUsage bufusage_start, bufusage;
		struct PartialExplainContext ctx = {
			.into = into,
			.es = es,
			.queryEnv = queryEnv
		};

		if (es->buffers)
			bufusage_start = pgBufferUsage;
		INSTR_TIME_SET_CURRENT(planstart);

		// Do this when only EXPLAIN (and no ANALYZE).
		if (hypocost_enable &&
		    hypocost_alter_explain &&
		    !hypocost_in_explain_analyze &&
		    (es->format == EXPLAIN_FORMAT_TEXT || es->format == EXPLAIN_FORMAT_JSON))
		{
			// Inject the explain context.
			es_ctx = &ctx;
			if (es->format == EXPLAIN_FORMAT_TEXT)
			{
				// Insert a divider comment.
				appendStringInfoString(es->str, "Original Plan:\n");
			}
			else
			{
				ExplainOpenGroup("Plans", NULL, false, es);
			}
		}

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

		if (es_ctx != NULL)
		{
			if (es->format == EXPLAIN_FORMAT_TEXT)
			{
				// Insert a divider comment.
				appendStringInfoString(es->str, "\nRecosted Plan:\n");
			}
		}

		/* run it (if needed) and produce output */
		ExplainOnePlan(plan, into, es, queryString, params, queryEnv, &planduration, (es->buffers ? &bufusage : NULL));

		if (es_ctx != NULL)
		{
			if (es->format == EXPLAIN_FORMAT_JSON)
			{
				ExplainCloseGroup("Plans", NULL, false, es);
			}
			es_ctx = NULL;
		}
	}
        PG_FINALLY();
        {
		es_ctx = NULL;
        }
        PG_END_TRY();
}
