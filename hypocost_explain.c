#include "hypocost.h"

bool hypocost_in_explain = false;

void
hypocost_explain(Query *query, int cursorOptions,
				 IntoClause *into, ExplainState *es,
				 const char *queryString, ParamListInfo params,
				 QueryEnvironment *queryEnv)
{
        hypocost_in_explain = hypocost_enable;
        PG_TRY();
        {
                ExplainOneQuery_hook = prev_explain_hook;
                ExplainOneQuery(query, cursorOptions, into, es, queryString, params, queryEnv);
        }
        PG_FINALLY();
        {
                // Rehook.
                prev_explain_hook = ExplainOneQuery_hook;
                ExplainOneQuery_hook = hypocost_explain;
                hypocost_in_explain = false;
        }
        PG_END_TRY();
}
