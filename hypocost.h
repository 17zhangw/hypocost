#ifndef __HYPOCOST_H__
#define __HYPOCOST_H__

#include "postgres.h"
#include "fmgr.h"
#include "optimizer/planner.h"
#include "commands/explain.h"
#include "nodes/primnodes.h"

/** Hooks */
void hypocost_explain(Query *query, int cursorOptions, IntoClause *into, ExplainState *es, const char *queryString, ParamListInfo params, QueryEnvironment *queryEnv);
void hypocost_scribble(PlannerInfo* root, Path* path);
SubPlan* hypocost_pick_altsubplan(PlannerInfo* root, List* subplans);
PlannedStmt* hypocost_planner(Query *parse, const char* query_string, int cursorOptions, ParamListInfo boundParams);

void hypocost_check_substitute(PlannerInfo* root, IndexPath* ipath, Path* outer);
List* hypocost_check_replace(PlannerInfo* root, Path* path, bool inc_pk);
void hypocost_substitute_bpath(PlannerInfo* root, Path* path, List* oids);

extern bool hypocost_enable;
extern bool hypocost_alter_explain;
extern bool hypocost_inject_analyze;
extern bool hypocost_substitute;
extern bool hypocost_in_explain_analyze;
extern bool hypocost_do_scribble;

extern double hypocost_seq_page_cost;
extern double hypocost_random_page_cost;

struct PartialExplainContext
{
	IntoClause* into;
	ExplainState *es;
	QueryEnvironment *queryEnv;
};

extern struct PartialExplainContext *es_ctx;

#endif
