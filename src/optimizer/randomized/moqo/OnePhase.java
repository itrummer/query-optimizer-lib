package optimizer.randomized.moqo;

import cost.MultiCostModel;
import optimizer.randomized.RandomizedOptimizer;
import plans.Plan;
import plans.spaces.PlanSpace;
import queries.Query;

/**
 * Generic local search algorithm with one search phase. This class realizes for instance 
 * iterated improvement and simulated annealing when passing the appropriate phase 
 * implementations.
 * <p>
 * Attention: need to pass different phase objects into each <code>OnePhase</code> object
 * since the phases might maintain internal state.
 * 
 * @author immanueltrummer
 *
 */
public class OnePhase extends RandomizedOptimizer {
	final Phase phase;
	Plan currentPlan;
	
	public OnePhase(Phase phase) {
		this.phase = phase;
	}

	@Override
	protected void refineApproximation(Query query,
			boolean[] consideredMetrics, PlanSpace planSpace,
			MultiCostModel costModel, 
			int algIndex, int sizeIndex, int queryIndex) {
		// Generate next plan and add to frontier if not null
		currentPlan = phase.nextPlan(currentPlan);
		if (currentPlan != null) {
			addToFrontier(query, currentPlan, consideredMetrics);
		}
	}

	@Override
	protected void init(Query query, boolean[] consideredMetrics,
			PlanSpace planSpace, MultiCostModel costModel) {
		currentPlan = null;
		phase.init(query, consideredMetrics, planSpace, costModel);
	}

	@Override
	public void cleanUp() {
	}

	@Override
	public String toString() {
		return "1P:" + phase.toString();
	}
	/**
	 * No algorithm specific features.
	 */
	@Override
	protected void storeSpecificStatistics(int algIndex, int sizeIndex,
			int queryIndex) {
	}
}
