package optimizer.randomized.moqo;

import cost.MultiCostModel;
import plans.Plan;
import plans.spaces.PlanSpace;
import queries.Query;
import util.LocalSearchUtil;
import util.MathUtil;

/**
 * Implements the SAIO variant of simulated annealing described by Steinbrunn et al. VLDB '97.
 * We consider the search space of bushy query plans. The initial plan is randomly generated
 * and the initial temperature is set to twice the average cost of the initial plan. A certain
 * number of random moves are tried for each query plan before reducing the temperature. The
 * system is considered frozen once the temperature has reached a value of less than one.
 * 
 * @author immanueltrummer
 *
 */
public class AnnealingPhaseSAIO extends Phase {
	/**
	 * The initial temperature is set by scaling the cost of the initial solution by that factor.
	 */
	final double initTemperatureScale;
	/**
	 * The default number of moves to try before reducing the temperature is scaled up by
	 * this factor.
	 */
	final int nrTriesScale;
	/**
	 * The annealing temperature used to balance the exploration-exploitation tradeoff
	 */
	double temperature = -1;
	/**
	 * The number of moves to try with the same temperature.
	 */
	int chainLength = -1;
	/**
	 * Number of moves tried with the current temperature - used to check when it is time to
	 * reduce the temperature.
	 */
	int nrMovesWithCurrentTemperature = -1;
	
	/**
	 * This constructor allows to set the initial temperature scale explicitly.
	 * 
	 * @param nrTriesScale			default number of random moves before declaring local optimum
	 * @param initTemperatureScale	multiply initial plan cost by that factor to get initial
	 * 								temperature. The default for stand-alone annealing is 2.0
	 */
	public AnnealingPhaseSAIO(int nrTriesScale, double initTemperatureScale) {
		this.initTemperatureScale = initTemperatureScale;
		this.nrTriesScale = nrTriesScale;
	}
	/**
	 * This constructor sets the default value of 2.0 for the inital temperature scale.
	 * 
	 * @param nrTriesScale	the default number of moves to try before assuming a local
	 * 						optimum is scaled by this factor.
	 */
	public AnnealingPhaseSAIO(int nrTriesScale) {
		this(nrTriesScale, 2.0);
	}
	/**
	 * Calculates the chain length and sets the standard fields.
	 */
	@Override
	public void init(Query query, boolean[] consideredMetrics, 
			PlanSpace planSpace, MultiCostModel costModel) {
		super.init(query, consideredMetrics, planSpace, costModel);
		temperature = -1;
		int nrJoins = query.nrTables - 1;
		chainLength = 16 * nrJoins * nrTriesScale;
	}
	/**
	 * Either continues working with a plan generated in the last phase or randomly
	 * generates a new query plan if the current plan is a null pointer. Then tries
	 * a limited number of random moves from the current plan before reducing the
	 * temperature.
	 */
	@Override
	public Plan nextPlan(Plan currentPlan) {
		assert(chainLength > 0);
		boolean planInitialized = false;
		// Initialize current plan if necessary
		if (currentPlan == null) {
			// Plan needs to be initialized
			currentPlan = LocalSearchUtil.randomBushyPlan(query, planSpace);
			costModel.updateAll(currentPlan);
			planInitialized = true;
		}
		// Initialize temperature if necessary
		if (temperature < 1) {
			double avgCost = MathUtil.aggMean(currentPlan.getCostValuesCopy());
			temperature = Math.max(initTemperatureScale * avgCost, 1);
			nrMovesWithCurrentTemperature = 0;
		}
		assert(temperature >= 1);
		// Return new plan if freshly initialized
		if (planInitialized) {
			return currentPlan;
		}
		// Try more random moves until either the temperature needs to be reduced
		// or an improvement was reached.
		while (nrMovesWithCurrentTemperature < chainLength) {
			Plan randomPlan = LocalSearchUtil.randomMove(query, currentPlan, planSpace, costModel);
			++nrMovesWithCurrentTemperature;
			double[] curCost = currentPlan.getCostValuesCopy();
			double[] newCost = randomPlan.getCostValuesCopy();
			if (LocalSearchUtil.acceptMove(
					curCost, newCost, consideredMetrics, true, temperature)) {
				return randomPlan;
			}
		}
		// If we arrive here then the maximal number of moves for the current temperature
		// has been executed.
		temperature = 0.95 * temperature;
		nrMovesWithCurrentTemperature = 0;
		// Check if system is frozen and force re-initialization in that case
		if (temperature < 1) {
			return null;
		}
		return currentPlan;	
	}
	@Override
	public String toString() {
		return "SAIO";
	}
}
