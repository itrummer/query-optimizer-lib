package optimizer.randomized.moqo;


import cost.MultiCostModel;
import optimizer.randomized.RandomizedOptimizer;
import plans.Plan;
import plans.spaces.PlanSpace;
import queries.Query;
import util.LocalSearchUtil;

public class FastClimber extends RandomizedOptimizer {
	
	final boolean climbFast;
	
	public FastClimber(boolean climbFast) {
		this.climbFast = climbFast;
	}

	@Override
	protected void refineApproximation(Query query,
			boolean[] consideredMetrics, PlanSpace planSpace,
			MultiCostModel costModel, 
			int algIndex, int sizeIndex, int queryIndex) {
		// Generate random query plan
		Plan plan = LocalSearchUtil.randomBushyPlan(query, planSpace);
		costModel.updateAll(plan);
		// Improve plan via some version of multi-objective hill climbing
		if (climbFast) {
			plan = LocalSearchUtil.ParetoClimb(
					query, plan, planSpace, costModel, consideredMetrics);
		} else {
			plan = LocalSearchUtil.exhaustivePlanClimbing(
					query, plan, planSpace, costModel, consideredMetrics, null);
		}
		// Add improved plan to the frontier
		addToFrontier(query, plan, consideredMetrics);
	}

	@Override
	protected void init(Query query, boolean[] consideredMetrics,
			PlanSpace planSpace, MultiCostModel costModel) {
	}

	@Override
	public void cleanUp() {
	}
	/**
	 * No algorithm-specific features.
	 */
	@Override
	protected void storeSpecificStatistics(int algIndex, int sizeIndex,
			int queryIndex) {
	}

}
