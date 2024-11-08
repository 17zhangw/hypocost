#include "hypocost.h"
#include "optimizer/optimizer.h"
#include "jit/jit.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "optimizer/cost.h"
#include "optimizer/planmain.h"
#include "access/parallel.h"
#include "catalog/pg_proc.h"
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/subselect.h"
#include "optimizer/paths.h"
#include "partitioning/partdesc.h"
#include "nodes/nodeFuncs.h"


bool hypocost_do_scribble = false;
size_t valid_subplan_ids_len = 0;
int* valid_subplan_ids = NULL;


struct GUCState {
		double seq_page_cost;
		double random_page_cost;
		bool enable_hashjoin;
		bool enable_mergejoin;
		bool enable_nestloop;
		bool enable_sort;
		bool enable_gathermerge;
		bool enable_hashagg;
		bool enable_parallel_hash;
		bool enable_material;
		bool enable_memoize;
		bool enable_seqscan;
		bool enable_indexscan;
		bool enable_indexonlyscan;
		bool enable_bitmapscan;

		// We don't care about any knob that influences *only* runtime.
		//
		// We also don't try to preserve the "system state".
		// Effectively, this cost depends on the system state (e.g., work_mem).
};

static struct GUCState
save_state(void)
{
		struct GUCState save = {
				.seq_page_cost = seq_page_cost,
				.random_page_cost = random_page_cost,
				.enable_hashjoin = enable_hashjoin,
				.enable_mergejoin = enable_mergejoin,
				.enable_nestloop = enable_nestloop,
				.enable_sort = enable_sort,
				.enable_gathermerge = enable_gathermerge,
				.enable_hashagg = enable_hashagg,
				.enable_parallel_hash = enable_parallel_hash,
				.enable_material = enable_material,
				.enable_memoize = enable_memoize,
				.enable_seqscan = enable_seqscan,
				.enable_indexscan = enable_indexscan,
				.enable_indexonlyscan = enable_indexonlyscan,
				.enable_bitmapscan = enable_bitmapscan
		};

		// TODO: let's manually just turn everything on!
		seq_page_cost = hypocost_seq_page_cost;
		random_page_cost = hypocost_random_page_cost;
		enable_hashjoin = true;
		enable_mergejoin = true;
		enable_nestloop = true;
		enable_sort = true;
		enable_gathermerge = true;
		enable_hashagg = true;
		enable_parallel_hash = true;
		// Turn this off because it *doesn't* prevent a plan from emerging.
		// But it can distort the selected plan.
		// enable_material = true;
		enable_memoize = true;
		enable_seqscan = true;
		enable_indexscan = true;
		enable_indexonlyscan = true;
		enable_bitmapscan = true;

		// Don't tamper with this as it would alter query plans..
		// hash_mem_multiplier =
		return save;
}

static void
restore_state(struct GUCState s)
{
		seq_page_cost = s.seq_page_cost;
		random_page_cost = s.random_page_cost;
		enable_hashjoin = s.enable_hashjoin;
		enable_mergejoin = s.enable_mergejoin;
		enable_nestloop = s.enable_nestloop;
		enable_sort = s.enable_sort;
		enable_gathermerge = s.enable_gathermerge;
		enable_hashagg = s.enable_hashagg;
		enable_parallel_hash = s.enable_parallel_hash;
		enable_material = s.enable_material;
		enable_memoize = s.enable_memoize;
		enable_seqscan = s.enable_seqscan;
		enable_indexscan = s.enable_indexscan;
		enable_indexonlyscan = s.enable_indexonlyscan;
		enable_bitmapscan = s.enable_bitmapscan;
}


static bool
erase_restrictinfo_cost(Node *node, void* ctx)
{
		if (node == NULL)
				return false;

		if (IsA(node, RestrictInfo))
		{
				RestrictInfo *rinfo = (RestrictInfo *) node;
				rinfo->eval_cost.startup = -1;
				rinfo->eval_cost.per_tuple = 0;
				if (rinfo->orclause)
					erase_restrictinfo_cost((Node *) rinfo->orclause, NULL);
				else
					erase_restrictinfo_cost((Node *) rinfo->clause, NULL);
				return false;
		}

		return expression_tree_walker(node, erase_restrictinfo_cost, NULL);
}


static void
recompute_pathcosts(PlannerInfo* root, Path* path, struct GUCState* s)
{
		const char* plantype = NULL;
		/* Guard against stack overflow due to overly complex plans */
		check_stack_depth();

		switch (path->pathtype)
		{
				case T_SeqScan:
						cost_seqscan(path, root, path->parent, path->param_info);
						break;
				case T_SampleScan:
						cost_samplescan(path, root, path->parent, path->param_info);
						break;
				case T_IndexScan:
				case T_IndexOnlyScan: {
						IndexPath* ipath = (IndexPath*)path;
						if (ipath->indexinfo)
						{
								ListCell* l;
								foreach(l, ipath->indexinfo->indrestrictinfo)
								{
										erase_restrictinfo_cost(lfirst(l), NULL);
								}
						}

						cost_index(
								ipath,
								root,
								ipath->loop_count,
								ipath->partial_path
						);
						break;
				}
				case T_BitmapOr: {
						ListCell   *lc;
						BitmapOrPath* bpath = NULL;
						Assert(IsA(path, BitmapOrPath));
						bpath = (BitmapOrPath*)path;
						foreach(lc, bpath->bitmapquals)
						{
								Path *bitmapqual = (Path *) lfirst(lc);
								recompute_pathcosts(root, bitmapqual, s);
						}
						cost_bitmap_or_node(bpath, root);
						break;
				}
				case T_BitmapAnd: {
						ListCell   *lc;
						BitmapAndPath* bpath = NULL;
						Assert(IsA(path, BitmapAndPath));
						bpath = (BitmapAndPath*)path;
						foreach(lc, bpath->bitmapquals)
						{
								Path *bitmapqual = (Path *) lfirst(lc);
								recompute_pathcosts(root, bitmapqual, s);
						}
						cost_bitmap_and_node(bpath, root);
						break;
				}
				case T_BitmapHeapScan:
						recompute_pathcosts(root, ((BitmapHeapPath*)path)->bitmapqual, s);
						cost_bitmap_heap_scan(
								path,
								root,
								path->parent,
								path->param_info,
								((BitmapHeapPath*)path)->bitmapqual,
								((BitmapHeapPath*)path)->loop_count
						);
						break;
				case T_SubqueryScan:
						SubqueryScanPath *spath = NULL;
						Assert(IsA(path, SubqueryScanPath));
						spath = (SubqueryScanPath*)path;

						// In theory, this is its own "query".
						hypocost_scribble(path->parent->subroot, spath->subpath);

						cost_subqueryscan(spath, root, path->parent, path->param_info);
						break;
				case T_CteScan:
						cost_ctescan(path, root, path->parent, path->param_info);
						break;
				case T_HashJoin: {
						JoinCostWorkspace workspace;
						recompute_pathcosts(root, ((HashPath*)path)->jpath.outerjoinpath, s);
						recompute_pathcosts(root, ((HashPath*)path)->jpath.innerjoinpath, s);
						initial_cost_hashjoin(
								root,
								&workspace,
								((HashPath*)path)->jpath.jointype,
								((HashPath*)path)->path_hashclauses,
								((HashPath*)path)->jpath.outerjoinpath,
								((HashPath*)path)->jpath.innerjoinpath,
								&((HashPath*)path)->extra,
								((HashPath*)path)->parallel_hash
						);
						final_cost_hashjoin(
								root,
								(HashPath*)path,
								&workspace,
								&((HashPath*)path)->extra
						);
						break;
				}
				case T_MergeJoin: {
						JoinCostWorkspace workspace;
						// Save+Restore this because this is a costing decision made in costing...
						bool materialize_inner = ((MergePath*)path)->materialize_inner;
						recompute_pathcosts(root, ((MergePath*)path)->jpath.outerjoinpath, s);
						recompute_pathcosts(root, ((MergePath*)path)->jpath.innerjoinpath, s);
						initial_cost_mergejoin(
								root,
								&workspace,
								((MergePath*)path)->jpath.jointype,
								((MergePath*)path)->path_mergeclauses,
								((MergePath*)path)->jpath.outerjoinpath,
								((MergePath*)path)->jpath.innerjoinpath,
								((MergePath*)path)->outersortkeys,
								((MergePath*)path)->innersortkeys,
								&((MergePath*)path)->extra
						);

						final_cost_mergejoin(
								root,
								(MergePath*)path,
								&workspace,
								&((MergePath*)path)->extra,
								materialize_inner
						);
						break;
				}
				case T_NestLoop: {
						JoinCostWorkspace workspace;
						recompute_pathcosts(root, ((NestPath*)path)->jpath.outerjoinpath, s);
						recompute_pathcosts(root, ((NestPath*)path)->jpath.innerjoinpath, s);
						initial_cost_nestloop(
								root,
								&workspace, 
								((NestPath*)path)->jpath.jointype,
								((NestPath*)path)->jpath.outerjoinpath,
								((NestPath*)path)->jpath.innerjoinpath,
								&((NestPath*)path)->extra
						);
						final_cost_nestloop(
								root,
								(NestPath*)path,
								&workspace,
								&((NestPath*)path)->extra
						);
						break;
				}
				case T_Append:
						ListCell   *l;
						foreach(l, ((AppendPath*)path)->subpaths)
						{
								Path *subpath = (Path *) lfirst(l);
								recompute_pathcosts(root, subpath, s);
						}

						if (list_length(((AppendPath*)path)->subpaths) == 1)
						{
								Path *child = (Path *) linitial(((AppendPath*)path)->subpaths);
								path->rows = child->rows;
								path->startup_cost = child->startup_cost;
								path->total_cost = child->total_cost;
								((AppendPath*)path)->path.pathkeys = child->pathkeys;
						}
						else
								cost_append((AppendPath*)path);
						break;
				case T_Result:
						if (IsA(path, ProjectionPath))
						{
								ProjectionPath* ppath = (ProjectionPath*)path;
								Assert(IsA(path, ProjectionPath));
								recompute_pathcosts(root, ppath->subpath, s);
								if (is_projection_capable_path(ppath->subpath) || equal(ppath->subpath->pathtarget->exprs, path->pathtarget->exprs))
								{
										path->rows = ppath->subpath->rows;
										path->startup_cost = ppath->subpath->startup_cost + (path->pathtarget->cost.startup - ppath->subpath->pathtarget->cost.startup);

										path->total_cost = ppath->subpath->total_cost + (path->pathtarget->cost.startup - ppath->subpath->pathtarget->cost.startup);
										path->total_cost += (path->pathtarget->cost.per_tuple - ppath->subpath->pathtarget->cost.per_tuple) * ppath->subpath->rows;
								}
								else
								{
										path->rows = ppath->subpath->rows;
										path->startup_cost = ppath->subpath->startup_cost + path->pathtarget->cost.startup;

										path->total_cost = ppath->subpath->total_cost + path->pathtarget->cost.startup;
										path->total_cost += (cpu_tuple_cost + path->pathtarget->cost.per_tuple) * ppath->subpath->rows;
								}
						}
						else if (IsA(path, MinMaxAggPath))
						{
								ereport(ERROR,
										(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
										 errmsg("Unsupported recosting MinMaxAggPath")));
								break;
						}
						else if (IsA(path, GroupResultPath))
						{
								ereport(ERROR,
										(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
										 errmsg("Unsupported recosting GroupResultPath")));
								break;
						}
						else
						{
								/* Simple RTE_RESULT base relation */
								Assert(IsA(path, Path));
								cost_resultscan(
										path,
										root,
										path->parent,
										path->param_info
								);
						}
						break;
				case T_Unique:
						if (IsA(path, UpperUniquePath))
						{
								UpperUniquePath *upath = (UpperUniquePath*)path;
								recompute_pathcosts(root, upath->subpath, s);
								path->startup_cost = upath->subpath->startup_cost;
								path->total_cost = upath->subpath->total_cost + cpu_operator_cost * upath->subpath->rows * upath->numkeys;
						}
						else
						{
								UniquePath *uq = NULL;
								Path *cuq = NULL;
								Assert(IsA(path, UniquePath));
								recompute_pathcosts(root, ((UniquePath*)path)->subpath, s);

								cuq = path->parent->cheapest_unique_path;
								path->parent->cheapest_unique_path = NULL;
								uq = create_unique_path(
										root,
										path->parent,
										((UniquePath*)path)->subpath,
										((UniquePath*)path)->sjinfo
								);
								path->parent->cheapest_unique_path = cuq;

								Assert(uq != NULL);
								path->startup_cost = uq->path.startup_cost;
								path->total_cost = uq->path.total_cost;
						}
						break;
				case T_GatherMerge: {
						double *rows = NULL;
						Cost		input_startup_cost = 0;
						Cost		input_total_cost = 0;
						recompute_pathcosts(root, ((GatherMergePath*)path)->subpath, s);
						if (pathkeys_contained_in(((GatherMergePath*)path)->path.pathkeys, ((GatherMergePath*)path)->subpath->pathkeys))
						{
								/* Subpath is adequately ordered, we won't need to sort it */
								input_startup_cost += ((GatherMergePath*)path)->subpath->startup_cost;
								input_total_cost += ((GatherMergePath*)path)->subpath->total_cost;
						}
						else
						{
								/* We'll need to insert a Sort node, so include cost for that */
								Path		sort_path;	/* dummy for result of cost_sort */
								cost_sort(
										&sort_path,
										root,
										((GatherMergePath*)path)->path.pathkeys,
										((GatherMergePath*)path)->subpath->total_cost,
										((GatherMergePath*)path)->subpath->rows,
										((GatherMergePath*)path)->subpath->pathtarget->width,
										0.0,
										work_mem,
										-1
								);
								input_startup_cost += sort_path.startup_cost;
								input_total_cost += sort_path.total_cost;
						}

						if (((GatherMergePath*)path)->override_rows_valid)
						{
								rows = &((GatherMergePath*)path)->override_rows;
						}

						cost_gather_merge(
								(GatherMergePath*)path,
								root,
								path->parent,
								path->param_info,
								input_startup_cost,
								input_total_cost,
								rows
						);
						break;
				}
				case T_Gather: {
						double *rows = NULL;
						recompute_pathcosts(root, ((GatherPath*)path)->subpath, s);
						if (((GatherPath*)path)->override_rows_valid)
						{
								rows = &((GatherPath*)path)->override_rows;
						}

						cost_gather(
								(GatherPath*)path,
								root,
								path->parent,
								path->param_info,
								rows
						);
						break;
				}
				case T_Memoize:
						MemoizePath* mpath = (MemoizePath*)path;
						recompute_pathcosts(root, ((MemoizePath*)path)->subpath, s);
						mpath->path.startup_cost = mpath->subpath->startup_cost + cpu_tuple_cost;
						mpath->path.total_cost = mpath->subpath->total_cost + cpu_tuple_cost;
						mpath->path.rows = mpath->subpath->rows;
						break;
				case T_Material:
						recompute_pathcosts(root, ((MaterialPath*)path)->subpath, s);
						cost_material(
								path,
								((MaterialPath*)path)->subpath->startup_cost,
								((MaterialPath*)path)->subpath->total_cost,
								((MaterialPath*)path)->subpath->rows,
								((MaterialPath*)path)->subpath->pathtarget->width
						);
						break;
				case T_Sort:
						recompute_pathcosts(root, ((SortPath*)path)->subpath, s);
						cost_sort(
								path,
								root,
								((SortPath*)path)->path.pathkeys,
								((SortPath*)path)->subpath->total_cost,
								((SortPath*)path)->subpath->rows,
								((SortPath*)path)->subpath->pathtarget->width,
								0.0,
								work_mem,
								((SortPath*)path)->limit_tuples
						);
						break;
				case T_IncrementalSort:
						recompute_pathcosts(root, ((IncrementalSortPath*)path)->spath.subpath, s);
						cost_incremental_sort(
								path,
								root,
								((IncrementalSortPath*)path)->spath.path.pathkeys,
								((IncrementalSortPath*)path)->nPresortedCols,
								((IncrementalSortPath*)path)->spath.subpath->startup_cost,
								((IncrementalSortPath*)path)->spath.subpath->total_cost,
								((IncrementalSortPath*)path)->spath.path.rows,
								((IncrementalSortPath*)path)->spath.path.pathtarget->width,
								0.0,
								work_mem,
								((IncrementalSortPath*)path)->limit_tuples
						);
						break;
				case T_Group:
						Assert(IsA(path, GroupPath));
						recompute_pathcosts(root, ((GroupPath*)path)->subpath, s);
						cost_group(
								path,
								root,
								list_length(((GroupPath*)path)->groupClause),
								((GroupPath*)path)->num_groups,
								((GroupPath*)path)->qual,
								((GroupPath*)path)->subpath->startup_cost,
								((GroupPath*)path)->subpath->total_cost,
								((GroupPath*)path)->subpath->rows
						);
						path->startup_cost += path->pathtarget->cost.startup;
						path->total_cost += path->pathtarget->cost.startup + path->pathtarget->cost.per_tuple * path->rows;
						break;
				case T_Agg:
						if (IsA(path, GroupingSetsPath))
						{
								GroupingSetsPath *gp = (GroupingSetsPath*)path;
								AggClauseCosts *costs = NULL;
								recompute_pathcosts(root, ((GroupingSetsPath*)path)->subpath, s);
								if (((GroupingSetsPath*)gp)->aggcosts_valid)
										costs = &((GroupingSetsPath*)gp)->aggcosts;

								gp = create_groupingsets_path(
										root,
										path->parent,
										((GroupingSetsPath*)path)->subpath,
										((GroupingSetsPath*)path)->qual,
										((GroupingSetsPath*)path)->aggstrategy,
										((GroupingSetsPath*)path)->rollups,
										costs,
										((GroupingSetsPath*)path)->num_groups
								);

								path->startup_cost = gp->path.startup_cost;
								path->total_cost = gp->path.total_cost;
						}
						else
						{
								AggClauseCosts *costs = NULL;
								Assert(IsA(path, AggPath));
								recompute_pathcosts(root, ((AggPath*)path)->subpath, s);
								if (((AggPath*)path)->aggcosts_valid)
										costs = &((AggPath*)path)->aggcosts;

								cost_agg(
										path,
										root,
										((AggPath*)path)->aggstrategy,
										costs,
										list_length(((AggPath*)path)->groupClause),
										((AggPath*)path)->numGroups,
										((AggPath*)path)->qual,
										((AggPath*)path)->subpath->startup_cost,
										((AggPath*)path)->subpath->total_cost,
										((AggPath*)path)->subpath->rows,
										((AggPath*)path)->subpath->pathtarget->width
								);
								path->startup_cost += path->pathtarget->cost.startup;
								path->total_cost += path->pathtarget->cost.startup + path->pathtarget->cost.per_tuple * path->rows;
						}
						break;
				case T_Limit: {
						LimitPath *lpath = (LimitPath*)path;
						recompute_pathcosts(root, ((LimitPath*)path)->subpath, s);
						lpath->path.rows = lpath->subpath->rows;
						lpath->path.startup_cost = lpath->subpath->startup_cost;
						lpath->path.total_cost= lpath->subpath->total_cost;

						adjust_limit_rows_costs(
								&path->rows,
								&path->startup_cost,
								&path->total_cost,
								((LimitPath*)path)->offset_est,
								((LimitPath*)path)->count_est
						);
						break;
				}
				case T_SetOp: {
						SetOpPath *spath = (SetOpPath*)path;
						recompute_pathcosts(root, spath->subpath, s);
						spath->path.startup_cost = spath->subpath->startup_cost;
						spath->path.total_cost = spath->subpath->total_cost + cpu_operator_cost * spath->subpath->rows * list_length(spath->distinctList);
						break;
				}
				case T_TidScan:
						plantype = plantype ? plantype : "TidScan";
						[[fallthrough]];
				case T_TidRangeScan:
						plantype = plantype ? plantype : "TidRangeScan";
						[[fallthrough]];
				case T_WorkTableScan:
						plantype = plantype ? plantype : "WorkTableScan";
						[[fallthrough]];
				case T_NamedTuplestoreScan:
						plantype = plantype ? plantype : "NamedTuplestoreScan";
						[[fallthrough]];
				case T_ForeignScan:
						plantype = plantype ? plantype : "ForeignScan";
						[[fallthrough]];
				case T_CustomScan:
						plantype = plantype ? plantype : "CustomScan";
						[[fallthrough]];
				case T_FunctionScan:
						plantype = plantype ? plantype : "FunctionScan";
						[[fallthrough]];
				case T_TableFuncScan:
						plantype = plantype ? plantype : "TableFuncScan";
						[[fallthrough]];
				case T_ValuesScan:
						plantype = plantype ? plantype : "ValuesScan";
						[[fallthrough]];
				case T_MergeAppend:
						plantype = plantype ? plantype : "MergeAppend";
						[[fallthrough]];
				case T_ModifyTable:
						plantype = plantype ? plantype : "ModifyTable";
						[[fallthrough]];
				case T_LockRows:
						plantype = plantype ? plantype : "LockRows";
						[[fallthrough]];
				case T_WindowAgg:
						plantype = plantype ? plantype : "WindowAgg";
						[[fallthrough]];
				case T_ProjectSet:
						plantype = plantype ? plantype : "ProjectSet";
						[[fallthrough]];
				case T_RecursiveUnion:
						plantype = plantype ? plantype : "RecursiveUnion";
						[[fallthrough]];
				default:
						plantype = plantype ? plantype : "Unknown";
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("Unsupported recosting %s", plantype)));
						break;
		}
}


SubPlan* hypocost_pick_altsubplan(PlannerInfo* root, List* subpaths)
{
		ListCell *lp;
		if (!hypocost_in_explain || !hypocost_do_scribble)
				// Don't intervene.
				return NULL;

		foreach(lp, subpaths)
		{
				Plan* sp = (Plan*)lfirst(lp);
				Assert(IsA(lfirst(lp), SubPlan));
				if (IsA(sp, SubPlan))
				{
						SubPlan* ssp = (SubPlan*)sp;
						Assert((ssp->plan_id - 1) < list_length(root->glob->subplans));
						Assert((ssp->plan_id - 1) < valid_subplan_ids_len);
						if (valid_subplan_ids[ssp->plan_id - 1])
						{
								return ssp;
						}
				}
		}

		return NULL;
}


static Plan*
process_subplan(PlannerGlobal* glob, SubPlan* sp)
{
		double		tuple_fraction;
		PlannerInfo *subroot;
		RelOptInfo *final_rel;
		Path	   *best_path;
		Plan* plan;
		struct GUCState s;
		Assert(sp->subLinkType != CTE_SUBLINK);
		if (sp->subLinkType == EXISTS_SUBLINK)
				tuple_fraction = 1.0;	/* just like a LIMIT 1 */
		else if (sp->subLinkType == ALL_SUBLINK || sp->subLinkType == ANY_SUBLINK)
				tuple_fraction = 0.5;	/* 50% */
		else
				tuple_fraction = 0.0;	/* default behavior */

		subroot = list_nth(glob->subroots, sp->plan_id - 1);
		final_rel = fetch_upper_rel(subroot, UPPERREL_FINAL, NULL);
		best_path = get_cheapest_fractional_path(final_rel, tuple_fraction);

		s = save_state();
		PG_TRY();
		{
				recompute_pathcosts(subroot, best_path, &s);
				plan = create_plan(subroot, best_path);
		}
		PG_FINALLY();
		{
				restore_state(s);
		}
		PG_END_TRY();
		return plan;
}

static Plan*
process_cte(PlannerGlobal* glob, SubPlan* sp)
{
		struct GUCState s;
		PlannerInfo *subroot;
		RelOptInfo *final_rel;
		Path	   *best_path;
		Plan* plan;

		/*
		 * From the postgres source code in subselect.c:
		 * Generate Paths for the CTE query.  Always plan for full retrieval
		 * --- we don't have enough info to predict otherwise.
		 */
		Assert(sp->subLinkType == CTE_SUBLINK);

		subroot = list_nth(glob->subroots, sp->plan_id - 1);
		final_rel = fetch_upper_rel(subroot, UPPERREL_FINAL, NULL);
		best_path = final_rel->cheapest_total_path;

		s = save_state();
		PG_TRY();
		{
				recompute_pathcosts(subroot, best_path, &s);
				plan = create_plan(subroot, best_path);
		}
		PG_FINALLY();
		{
				restore_state(s);
		}
		PG_END_TRY();
		return plan;
}


void hypocost_scribble(PlannerInfo* root, Path* path)
{
		struct GUCState s;
		List* nodes;
		ListCell *lc;
		if (!hypocost_in_explain || !hypocost_do_scribble)
				return;

		nodes = list_concat_copy(root->init_plans, root->noninit_plans);
		foreach (lc, nodes)
		{
				Plan* p = (Plan*)lfirst(lc);
				if (p != NULL && IsA(p, SubPlan))
				{
						Plan* plan;
						PlannerInfo* subroot = list_nth(root->glob->subroots, ((SubPlan*)p)->plan_id - 1);
						if (((SubPlan*)p)->subLinkType == CTE_SUBLINK)
								plan = process_cte(root->glob, (SubPlan*)p);
						else
								plan = process_subplan(root->glob, (SubPlan*)p);
						list_nth_cell(root->glob->subplans, ((SubPlan*)p)->plan_id-1)->ptr_value = plan;
						cost_subplan(subroot, (SubPlan*)p, plan);
				}
		}

		s = save_state();
		PG_TRY();
		{
				// Recompute the main.
				recompute_pathcosts(root, path, &s);

				// We need to do something similar to charge for init plans...
				// See: SS_charge_for_initplans.
				foreach (lc, root->init_plans)
				{
						SubPlan* isp = (SubPlan*)lfirst(lc);
						Assert(IsA(lfirst(lc), SubPlan));
						path->startup_cost += isp->startup_cost + isp->per_call_cost;
						path->total_cost += isp->startup_cost + isp->per_call_cost;
				}
		}
		PG_FINALLY();
		{
				restore_state(s);
		}
		PG_END_TRY();
}


PlannedStmt* hypocost_planner(Query *parse, const char* query_string, int cursorOptions, ParamListInfo boundParams)
{
		PlannedStmt* result = NULL;
		Query* cparse = NULL;
		ListCell* lc = NULL;
		if (!hypocost_in_explain)
		{
				return standard_planner(parse, query_string, cursorOptions, boundParams);
		}

		cparse = copyObject(parse);
		result = standard_planner(parse, query_string, cursorOptions, boundParams);
		if (result->subplans)
		{
				// Stash which subplan IDs are actually valid.
				valid_subplan_ids_len = list_length(result->subplans);
				valid_subplan_ids = MemoryContextAlloc(TopMemoryContext, sizeof(int)*valid_subplan_ids_len);
				memset(valid_subplan_ids, 0x00, sizeof(int)*valid_subplan_ids_len);
				foreach(lc, result->subplans)
				{
						int ndx = foreach_current_index(lc);
						Plan* p = (Plan*)lfirst(lc);
						if (p != NULL)
						{
								valid_subplan_ids[ndx] = true;
						}
				}
		}

		hypocost_do_scribble = true;
		PG_TRY();
		{
				result = standard_planner(cparse, query_string, cursorOptions, boundParams);
		}
		PG_FINALLY();
		{
				hypocost_do_scribble = false;
				if (valid_subplan_ids)
				{
						MemoryContext old = MemoryContextSwitchTo(TopMemoryContext);
						pfree(valid_subplan_ids);
						valid_subplan_ids = NULL;
						valid_subplan_ids_len = 0;
						MemoryContextSwitchTo(old);
				}

				return result;
		}
		PG_END_TRY();
}