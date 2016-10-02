package optimizer.randomized.moqo;

import cost.MultiCostModel;
import optimizer.randomized.RandomizedOptimizer;
import plans.Plan;
import plans.spaces.PlanSpace;
import queries.Query;

/**
 * Generic local search algorithm with two search phases. The two search phases are specified
 * as parameters and might for instance include local search and simulated annealing. We switch
 * from the first to the second phase after a certain number of null pointers (semantic: no
 * suitable plans found in the neighborhood of current plan) has been returned by the first
 * phase. The number of null pointers before the switch is specified in the constructor.
 * <p>
 * Attention: need to pass different phase objects into each <code>TwoPhase</code> object
 * since the phases might maintain internal state.
 * 
 * @author immanueltrummer
 *
 */
public class TwoPhase extends RandomizedOptimizer {
	/**
	 * How often the first phase can return a null pointer before switching to the second phase.
	 */
	final int nrRunsPhase1;
	final Phase phase1;
	final Phase phase2;
	/**
	 * How often the first phase already returned a null pointer for the current query.
	 */
	int phase1RunCtr;
	Plan currentPlan;
	
	public TwoPhase(int nrRunsPhase1, Phase phase1, Phase phase2) {
		this.nrRunsPhase1 = nrRunsPhase1;
		this.phase1 = phase1;
		this.phase2 = phase2;
	}
	/**
	 * Executes the first phase until the first phase generated a null pointer
	 * a pre-specified number of times. Then it selects the first plan of the
	 * frontier remaining after the first phase and uses it to initialize the
	 * second phase. The second phase is executed until the timeout.
	 * 
	 */
	@Override
	protected void refineApproximation(Query query,
			boolean[] consideredMetrics, PlanSpace planSpace,
			MultiCostModel costModel, 
			int algIndex, int sizeIndex, int queryIndex) {
		// Execute either phase 1 or phase 2 depending on run counter
		if (phase1RunCtr<nrRunsPhase1) {
			// Execute first phase step
			currentPlan = phase1.nextPlan(currentPlan);
			// A run ends once the returned plan is null, 
			// meaning that no improvements were possible.
			if (currentPlan == null) {
				++phase1RunCtr;
				// If the first phase just ended then replace null pointer by first frontier plan
				if (phase1RunCtr == nrRunsPhase1) {
					currentPlan = currentApproximation.iterator().next();
				}
			}
		} else {
			// Execute second phase step
			currentPlan = phase2.nextPlan(currentPlan);
		}
		// Add plan to frontier if it is not null
		if (currentPlan != null) {
			addToFrontier(query, currentPlan, consideredMetrics);
		}
	}

	@Override
	protected void init(Query query, boolean[] consideredMetrics,
			PlanSpace planSpace, MultiCostModel costModel) {
		phase1.init(query, consideredMetrics, planSpace, costModel);
		phase2.init(query, consideredMetrics, planSpace, costModel);
		phase1RunCtr = 0;
		currentPlan = null;
	}

	@Override
	public void cleanUp() {
	}
	
	@Override
	public String toString() {
		return "2P:" + phase1.toString() + "; " + phase2.toString();
	}
	/**
	 * No algorithm-specific features.
	 */
	@Override
	protected void storeSpecificStatistics(int algIndex, int sizeIndex,
			int queryIndex) {
	}
}
