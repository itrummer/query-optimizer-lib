package optimizer.randomized.moqo;

import cost.CostModel;
import cost.MultiCostModel;
import plans.Plan;
import plans.spaces.PlanSpace;
import queries.Query;
import util.LocalSearchUtil;

/**
 * Implements a multi-objective generalization of the SAH variant of simulated annealing
 * that was described in the SIGMOD '88 paper "Optimization of large join queries" by Swami.
 * The calculation of the number of moves to try before reducing temperature differs however.
 * 
 * @author immanueltrummer
 *
 */
public class AnnealingPhaseSAH extends Phase {
	/**
	 * The default number of moves to try before giving up on improving a plan is scaled
	 * by that factor.
	 */
	final int nrTriesScale;
	public AnnealingPhaseSAH(int nrTriesScale) {
		this.nrTriesScale = nrTriesScale;
	}
	/**
	 * Influences the likelihood of moving towards dominated plans.
	 */
	double temperature = -1;
	/**
	 * Cost distribution standard deviation - will be used to initialize and update temperature
	 */
	double costStDev = -1;
	/**
	 * Determine initial temperature for simulated annealing based on the estimated
	 * standard deviation of the cost distribution.
	 * 
	 * @param query				the query to optimize
	 * @param consideredMetrics Boolean flags indicating for each metric if it is considered
	 * @param planSpace			determines the set of scan and join operators to consider
	 * @param costModel			estimates query plan cost according to multiple metrics
	 */
	void initializeTemperature(
			Query query, boolean[] consideredMetrics, PlanSpace planSpace, CostModel costModel) {
		costStDev = LocalSearchUtil.estimateCostStDev(query, consideredMetrics, planSpace, costModel);
		temperature = 20 * costStDev;
	}
	/**
	 * Reduce the temperature to balance the tradeoff between exploration and exploitation.
	 */
	void updateTemperature() {
		assert(costStDev >= 0);
		assert(temperature >= 0.5);
		temperature = Math.max(0.5, Math.exp(-0.7*temperature/costStDev));
	}
	/**
	 * Initializes the current plan by a random plan and otherwise 
	 */
	@Override
	public Plan nextPlan(Plan currentPlan) {
		// Generate random plan at first invocation
		if (currentPlan == null) {
			currentPlan = LocalSearchUtil.randomBushyPlan(query, planSpace);
			costModel.updateAll(currentPlan);
		} else {
			// Try a certain number of times to move to a different acceptable plan
			boolean currentPlanReplaced = false;
			// The number of tries corresponds by default to the number of predicates 
			// which is linear in the number of tables for chain and star graphs.
			int nrTries = (query.nrTables - 1) * nrTriesScale;
			for (int tryCtr=0; tryCtr<nrTries; ++tryCtr) {
				Plan randomPlan = LocalSearchUtil.randomMove(
						query, currentPlan, planSpace, costModel);
				if (LocalSearchUtil.acceptMove(currentPlan.getCostValuesCopy(), 
						randomPlan.getCostValuesCopy(), consideredMetrics, true, temperature)) {
					currentPlan = randomPlan;
					currentPlanReplaced = true;
					break;
				}
			}
			if (!currentPlanReplaced) {
				// Reduce temperature
				updateTemperature();
			}
		}
		return currentPlan;
	}
	/**
	 * Initializes standard fields and calculates first temperature.
	 */
	@Override
	public void init(Query query, boolean[] consideredMetrics, 
			PlanSpace planSpace, MultiCostModel costModel) {
		super.init(query, consideredMetrics, planSpace, costModel);
		initializeTemperature(query, consideredMetrics, planSpace, costModel);
	}
	@Override
	public String toString() {
		return "SAH";
	}
}
