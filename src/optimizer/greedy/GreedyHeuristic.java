package optimizer.greedy;

import static common.Constants.NR_TIME_PERIODS;
import static common.Constants.TIMEOUT_MILLIS;

import java.util.LinkedList;
import java.util.List;

import benchmark.Statistics;
import cost.MultiCostModel;
import optimizer.Optimizer;
import plans.ParetoPlanSet;
import plans.Plan;
import plans.spaces.PlanSpace;
import queries.Query;
import util.GreedyCriterion;
import util.GreedyUtil;
import util.ParetoUtil;

/**
 * Simple heuristic that builds a query plan greedily by picking the next join
 * according to some simple criteria. For instance we can always prefer the join
 * that leads to the smallest new intermediate result. This heuristic was found to
 * work best among multiple similar greedy heuristics (such as always choosing the
 * join with maximal selectivity) by Bruno et al. (see ICDE'2010 paper "Polynomial
 * Heuristics for Query Optimization"). The heuristic measures the byte size of
 * intermediate results for the selection (as opposed to the cardinality).
 * Finally the heuristic uses hill climbing to reach the next local optimum from
 * the generated plan.
 * 
 * @author immanueltrummer
 *
 */
public class GreedyHeuristic extends Optimizer {
	/**
	 * This criterion is used to select the next join greedily.
	 */
	final GreedyCriterion greedyCriterion;
	
	public GreedyHeuristic(GreedyCriterion greedyCriterion) {
		this.greedyCriterion = greedyCriterion;
	}
	@Override
	public ParetoPlanSet approximateParetoSet(Query query,
			boolean[] consideredMetrics, PlanSpace planSpace,
			MultiCostModel costModel, ParetoPlanSet refPlanSet, int algIndex,
			int sizeIndex, int queryIndex) {
		// Take start time
		long startMillis = System.currentTimeMillis();
		// Greedily produce minSize plan
		Plan resultPlan = GreedyUtil.greedyPlan(query, planSpace, costModel, greedyCriterion);
		// Insert result plan into list for compatibility
		List<Plan> resultPlans = new LinkedList<Plan>();
		resultPlans.add(resultPlan);
		// Update statistics
		if (refPlanSet != null){
			long timePeriodMillis = TIMEOUT_MILLIS/NR_TIME_PERIODS;
			long millisPassed = System.currentTimeMillis() - startMillis;
			int curTimePeriod = (int)(millisPassed/timePeriodMillis);
			double curEpsilon = ParetoUtil.epsilonError(
					resultPlans, refPlanSet.plans, consideredMetrics);
			for (int periodCtr=0; periodCtr<NR_TIME_PERIODS; ++periodCtr) {
				String featureName = "Epsilon approximation after X-th time period";
				double epsilon = periodCtr >= curTimePeriod ? curEpsilon : Double.POSITIVE_INFINITY;
				Statistics.addToDoubleFeature(featureName, 
						algIndex, sizeIndex, periodCtr, queryIndex, epsilon);
			}
		}
		// Return result plan as Pareto frontier
		return new ParetoPlanSet(resultPlans);
	}
	@Override
	public String toString() {
		return "Greedy(" + greedyCriterion + ")";
	}
}
