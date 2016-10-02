package optimizer.randomized.moqo;

import cost.MultiCostModel;
import optimizer.randomized.RandomizedOptimizer;
import plans.Plan;
import plans.spaces.PlanSpace;
import queries.Query;
import util.LocalSearchUtil;

/**
 * Implements iterative improvement as described by Steinbrunn et al. in their VLDB'97 paper
 * "Heuristic and randomized optimization for the join ordering problem".
 * 
 * @author immanueltrummer
 *
 */
public class IterativeImprovement extends RandomizedOptimizer {
	/**
	 * Generate random bushy plan and improve it via randomized hill climbing.
	 */
	@Override
	protected void refineApproximation(Query query,
			boolean[] consideredMetrics, PlanSpace planSpace,
			MultiCostModel costModel, int algIndex, int sizeIndex,
			int queryIndex) {
		// Generate random bushy plan and calculate its cost
		Plan randomPlan = LocalSearchUtil.randomBushyPlan(query, planSpace);
		costModel.updateAll(randomPlan);
		// Reach local Pareto-optimum via randomized hill climbing
		Plan improvedPlan = LocalSearchUtil.randomizedParetoClimb(query, randomPlan, 
				planSpace, costModel, consideredMetrics);
		// Add resulting plan to approximation of Pareto frontier
		addToFrontier(query, improvedPlan, consideredMetrics);
	}
	/**
	 * No specific statistics to store.
	 */
	@Override
	protected void storeSpecificStatistics(int algIndex, int sizeIndex,
			int queryIndex) {
	}
	/**
	 * Nothing specific to initialize.
	 */
	@Override
	protected void init(Query query, boolean[] consideredMetrics,
			PlanSpace planSpace, MultiCostModel costModel) {
	}
	/**
	 * Nothing to be cleaned up afterwards.
	 */
	@Override
	public void cleanUp() {
	}

}
