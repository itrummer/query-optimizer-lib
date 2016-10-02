package optimizer.randomized.moqo;

import plans.Plan;
import util.GreedyCriterion;
import util.GreedyUtil;

/**
 * This phase creates a plan according to a greedy heuristic (for the moment
 * we use the minSize heuristic as it was found to be superior to other greedy 
 * heuristics by Bruno et al. in their paper "Polynomial heuristics for query
 * optimization" at ICDE 2010).
 */
public class GreedyPhase extends Phase {
	/**
	 * If no plan is currently known then a minSize plan is generated,
	 * otherwise a null pointer is returned to start the next phase.
	 */
	@Override
	public Plan nextPlan(Plan currentPlan) {
		if (currentPlan == null) {
			return GreedyUtil.greedyPlan(query, planSpace, costModel, GreedyCriterion.MIN_SIZE);
		} else {
			// Starts next phase if any
			return null;
		}
	}

}
