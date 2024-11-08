#ifndef __HYPOCOST_H__
#define __HYPOCOST_H__

#include "postgres.h"
#include "fmgr.h"
#include "optimizer/planner.h"
#include "commands/explain.h"

/** Hooks */
void hypocost_explain(Query *query, int cursorOptions, IntoClause *into, ExplainState *es, const char *queryString, ParamListInfo params, QueryEnvironment *queryEnv);
void hypocost_scribble(PlannerInfo* root, Path* path);
SubPlan* hypocost_pick_altsubplan(PlannerInfo* root, List* subplans);
PlannedStmt* hypocost_planner(Query *parse, const char* query_string, int cursorOptions, ParamListInfo boundParams);
extern ExplainOneQuery_hook_type prev_explain_hook;

extern bool hypocost_enable;
extern bool hypocost_in_explain;
extern bool hypocost_do_scribble;

extern double hypocost_seq_page_cost;
extern double hypocost_random_page_cost;

#endif
