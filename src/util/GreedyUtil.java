package util;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import cost.MultiCostModel;
import plans.JoinPlan;
import plans.Plan;
import plans.ScanPlan;
import plans.operators.JoinOperator;
import plans.spaces.PlanSpace;
import queries.Query;

/**
 * Contains several methods that are helpful for greedy approaches that build a
 * query plan greedily according to diverse criteria.
 * 
 * @author immanueltrummer
 *
 */
public class GreedyUtil {
	/**
	 * Checks whether one join plan is better than another one according to some simple
	 * greedy criterion that is used to select the next join. Assumes that both plans
	 * are no null pointers and does not check for that case.
	 * 
	 * @param better		the plan that is presumably better
	 * @param worse			the plan that is presumably worse
	 * @param criterion		the criterion according to which the plans are judged
	 * @return				true if the better plan is indeed better according to the given criterion
	 */
	static boolean better(JoinPlan better, JoinPlan worse, GreedyCriterion criterion) {
		switch (criterion) {
		case MIN_SIZE:
			return better.resultRel.pages * 10 < worse.resultRel.pages;
		case MIN_SELECTIVITY:
			double betterSelectivity = better.resultRel.cardinality/
				(better.getLeftPlan().resultRel.cardinality * better.getRightPlan().resultRel.cardinality);
			double worseSelectivity = worse.resultRel.cardinality/
				(worse.getLeftPlan().resultRel.cardinality * worse.getRightPlan().resultRel.cardinality);
			return betterSelectivity < worseSelectivity;
		default:
			assert(false);
			return false;
		}
	}
	/**
	 * Determines the join between two partial plans optimizing a simple criterion such
	 * as minimizing the intermediate result size. Adds the corresponding plan to the
	 * partial plans and removes its sub-plans.
	 * 
	 * @param query			the query being optimized
	 * @param partialPlans	a set of partial query plans
	 * @param planSpace		the plan space determining the applicable operators
	 * @param costModel		used to estimate the cost of query plans according to multiple metrics
	 * @param criterion		the criterion according to which the join is selected
	 */
	static void performGreedyJoin(Query query, List<Plan> partialPlans, 
			PlanSpace planSpace, MultiCostModel costModel, GreedyCriterion criterion) {
		Collections.shuffle(partialPlans);
		// Determine join leading to an intermediate result of minimal size.
		// This is the best possible join according to the minSize heuristic.
		JoinPlan bestJoin = null;
		for (Plan leftPlan : partialPlans) {
			for (Plan rightPlan : partialPlans) {
				if (leftPlan != rightPlan) {
					JoinOperator joinOperator = planSpace.randomJoinOperator(leftPlan, rightPlan);
					JoinPlan joinPlan = new JoinPlan(query, leftPlan, rightPlan, joinOperator);
					if (bestJoin == null) {
						bestJoin = joinPlan;
					} else if (better(joinPlan, bestJoin, criterion)) {
						bestJoin = joinPlan;
					}
				}
			}
		}
		// Calculate the cost of best plan
		costModel.updateRoot(bestJoin);
		// Insert best join plan and remove its sub-plans from partial plans list
		partialPlans.remove(bestJoin.getLeftPlan());
		partialPlans.remove(bestJoin.getRightPlan());
		partialPlans.add(bestJoin);
	}
	/**
	 * Creates a query plan by selecting joins greedily according to some simple criterion.
	 * 
	 * @param query		the query for which a plan is created
	 * @param planSpace	determines the set of applicable scan and join operators
	 * @param costModel	used to calculate cost of query plans according to different metrics
	 * @param criterion	the criterion according to which joins are selected
	 * @return			a greedily constructed query plan with its cost calculated
	 */
	public static Plan greedyPlan(Query query, PlanSpace planSpace, 
			MultiCostModel costModel, GreedyCriterion criterion) {
		int nrTables = query.nrTables;
		// Generate scan plans for all query tables
		List<Plan> partialPlans = new LinkedList<Plan>();
		for (int tableIndex=0; tableIndex<nrTables; ++tableIndex) {
			// We assume that there is only one default scan operator
			assert(planSpace.consideredScanOps.size() == 1);
			Plan scanPlan = new ScanPlan(query, tableIndex, planSpace.defaultScanOperator);
			costModel.updateRoot(scanPlan);
			partialPlans.add(scanPlan);
		}
		// Combine scan plans until we obtain a completed plan
		return greedyPlan(query, planSpace, costModel, partialPlans, criterion);
	}
	/**
	 * Creates a query plan joining the given list of partial plans by selecting the next
	 * join to take according to some simple criterion.
	 * 
	 * @param query			the query for which a partial plan is created
	 * @param planSpace		determines the set of applicable scan and join operators
	 * @param costModel		used to calculate cost of query plans according to different metrics
	 * @param partialPlans	those are the partial plans from which we combine a new partial plan
	 * @param criterion		the criterion according to which the next join to take is selected
	 * @return				a join between all given partial plans
	 */
	public static Plan greedyPlan(Query query, PlanSpace planSpace, MultiCostModel costModel, 
			List<Plan> partialPlans, GreedyCriterion criterion) {
		// Combine partial plans until we obtain a completed plan
		while (partialPlans.size() > 1) {
			performGreedyJoin(query, partialPlans, planSpace, costModel, criterion);
		}
		// Return completed plan
		return partialPlans.iterator().next();
	}
}
