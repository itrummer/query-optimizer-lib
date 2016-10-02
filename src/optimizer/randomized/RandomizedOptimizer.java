package optimizer.randomized;

import static common.Constants.*;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import queries.Query;
import util.LocalSearchUtil;
import util.ParetoUtil;
import util.PruningUtil;
import util.TestUtil;
import benchmark.Statistics;
import cost.MultiCostModel;
import optimizer.Optimizer;
import plans.spaces.PlanSpace;
import plans.ParetoPlanSet;
import plans.Plan;

/**
 * Generic randomized algorithm that iteratively refines the 
 * approximation of the Pareto frontier.
 * 
 * @author immanueltrummer
 *
 */
public abstract class RandomizedOptimizer extends Optimizer {
	/**
	 * Millis since optimization start for this query
	 */
	protected long startMillis;	
	/**
	 * Contains current approximation of Pareto frontier. 
	 * Must be cleared before a new query is optimized.
	 */
	public List<Plan> currentApproximation = new LinkedList<Plan>();
	/**
	 * Refine approximation of Pareto frontier. This method is called once per iteration
	 * and implements algorithm-specific logic to generate new plans refining the Pareto
	 * frontier approximation. 
	 * 
	 * @param query				the query whose Pareto plan frontier should be approximated
	 * @param consideredMetrics	Boolean flags indicating if certain metrics are considered
	 * @param planSpace			determines applicable scan and join operators
	 * @param costModel			used to calculate the cost of query plans
	 * @param algIndex			statistics are collected for that algorithm index
	 * @param sizeIndex			statistics are collected for that query size index
	 * @param queryIndex		index of query within its query size group
	 */
	protected abstract void refineApproximation(Query query, boolean[] consideredMetrics, 
			PlanSpace planSpace, MultiCostModel costModel, 
			int algIndex, int sizeIndex, int queryIndex);
	/**
	 * Add one new plan to the frontier approximation and prune. The new plan is copied
	 * to avoid inconsistencies if it is modified later.
	 * 
	 * @param query				the query being optimized
	 * @param plan				a new plan to consider for that query
	 * @param consideredMetric	Boolean flags indicating which cost metrics are considered
	 */
	protected void addToFrontier(Query query, Plan plan, boolean[] consideredMetric) {
		// Must make plan copy since local search might reuse nodes of original 
		// plan to build new plans.
		Plan planCopy = plan.deepMutableCopy();
		planCopy.makeImmutable();
		PruningUtil.pruneCostBased(currentApproximation, planCopy, consideredMetric);
	}
	/**
	 * This function allows algorithms to store statistics about algorithm-specific features.
	 * 
	 * @param algIndex			statistics are collected for that algorithm index
	 * @param sizeIndex			statistics are collected for that query size index
	 * @param queryIndex		index of query within its query size group
	 */
	protected abstract void storeSpecificStatistics(int algIndex, int sizeIndex, int queryIndex);
	/**
	 * Iteratively refines approximation of Pareto plan set for given query and records
	 * several statistics. The statistics include the epsilon error that represents
	 * approximation quality and is calculated by comparison with a reference plan set.
	 */
	@Override
	public ParetoPlanSet approximateParetoSet(Query query, boolean[] consideredMetrics, 
			PlanSpace planSpace, MultiCostModel costModel, ParetoPlanSet refPlanSet,
			int algIndex, int sizeIndex, int queryIndex) {
		Plan.nrPlansCreated = 0;
		costModel.nrRootCostEvaluations = 0;
		LocalSearchUtil.nrExhaustiveClimbs = 0;
		LocalSearchUtil.nrExhaustiveSteps = 0;
		LocalSearchUtil.accEpsilonImprovement = 0;
		currentApproximation.clear();
		// Register start time to check for timeouts
		startMillis = System.currentTimeMillis();
		// Approximation quality after x% of optimization time
		double[] epsilonAfterTimePeriod = new double[NR_TIME_PERIODS];
		Arrays.fill(epsilonAfterTimePeriod, Double.POSITIVE_INFINITY);
		init(query, consideredMetrics, planSpace, costModel);
		boolean timeout = false;
		long millisBetweenEpsilonUpdates = 50;
		long lastEpsilonUpdateMillis = 0;
		long iterationCtr = 0;
		// Iterate until timeout reached or highest epsilon value reached
		while (!timeout) {
			++iterationCtr;
			refineApproximation(query, consideredMetrics, planSpace, 
					costModel, algIndex, sizeIndex, queryIndex);
			// Check timeout
			long millisPassed = System.currentTimeMillis() - startMillis ;
			int curTimePeriod = (int)(millisPassed/TIME_PERIOD_MILLIS);
			if (millisPassed > TIMEOUT_MILLIS) {
				timeout = true;
			}
			// Make sure that we calculate epsilon value not too often to avoid performance impact
			if (curTimePeriod < NR_TIME_PERIODS && System.currentTimeMillis() - 
					lastEpsilonUpdateMillis >= millisBetweenEpsilonUpdates) {
				// Check approximation quality comparing with reference plan set
				if (refPlanSet != null) {
					double curEpsilon = ParetoUtil.epsilonError(
							currentApproximation, refPlanSet.plans, consideredMetrics);
					for (int periodCtr=curTimePeriod; periodCtr<NR_TIME_PERIODS; ++periodCtr) {
						epsilonAfterTimePeriod[periodCtr] = Math.min(
								epsilonAfterTimePeriod[periodCtr], curEpsilon);
					}
					lastEpsilonUpdateMillis = System.currentTimeMillis();
				}
				// Count refinement step
				{
					String featureName = "#Refinement steps in X-th period";
					Statistics.addToLongFeature(featureName, 
							algIndex, sizeIndex, curTimePeriod, queryIndex, 1);
				}				
			}
		}
		// Calculate aggregate statistics
		double nrSteps = LocalSearchUtil.nrExhaustiveSteps;
		double nrClimbs = LocalSearchUtil.nrExhaustiveClimbs;
		double climbImprovements = LocalSearchUtil.accEpsilonImprovement;
		double avgSteps = nrClimbs == 0 ? 0 : nrSteps/nrClimbs;
		double avgImprovement = nrSteps == 0 ? 0 : climbImprovements / nrSteps; 
		double finalEpsilon = refPlanSet == null ? -1 :
			ParetoUtil.epsilonError(currentApproximation, refPlanSet.plans, consideredMetrics);
		// Output statistics
		System.out.println("Executed " + iterationCtr + " iterations");
		System.out.println("Average climbing steps: " + avgSteps);
		System.out.println("Average improvement per step: " + avgImprovement);
		System.out.println("Final epsilon: " + finalEpsilon);
		// Verify that all generated plans are consistent and their cost was correctly calculated
		if (SAFE_MODE) {
			TestUtil.validatePlans(currentApproximation, planSpace, costModel, true);
		}
		// Update statistics
		{
			String featureName = "Final epsilon";
			Statistics.addToDoubleFeature(featureName, 
					algIndex, sizeIndex, 0, queryIndex, finalEpsilon);
		}
		{
			String featureName = "Total nr iterations";
			Statistics.addToLongFeature(featureName, 
					algIndex, sizeIndex, 0, queryIndex, iterationCtr);
		}
		{
			String featureName = "Average climbing path length";
			Statistics.addToDoubleFeature(featureName, 
					algIndex, sizeIndex, 0, queryIndex, avgSteps);
		}
		{
			String featureName = "Average improvement per climbing step";
			Statistics.addToDoubleFeature(featureName, 
					algIndex, sizeIndex, 0, queryIndex, avgImprovement);
		}
		{
			String featureName = "Epsilon approximation after X-th time period";
			for (int periodCtr=0; periodCtr<NR_TIME_PERIODS; ++periodCtr) {
				double epsilon = epsilonAfterTimePeriod[periodCtr];
				Statistics.addToDoubleFeature(featureName, 
						algIndex, sizeIndex, periodCtr, queryIndex, epsilon);
			}		
		}
		{
			String featureName = "#Pareto plans";
			long nrParetoPlans = currentApproximation.size();
			Statistics.addToDoubleFeature(featureName, 
					algIndex, sizeIndex, 0, queryIndex, nrParetoPlans);
		}
		{
			String featureName = "#Partial Plans Created";
			Statistics.addToLongFeature(featureName, 
					algIndex, sizeIndex, 0, queryIndex, Plan.nrPlansCreated);
		}
		{
			String featureName = "#Cost Evaluations for Plan Nodes";
			long nrRootCostEvaluations = costModel.nrRootCostEvaluations;
			Statistics.addToLongFeature(featureName, 
					algIndex, sizeIndex, 0, queryIndex, nrRootCostEvaluations);
		}
		// Gives each algorithm the occasion to store statistics about algorithm-specific features
		storeSpecificStatistics(algIndex, sizeIndex, queryIndex);
		// Let's each algorithm clean up algorithm-specific data structures
		cleanUp();
		return new ParetoPlanSet(currentApproximation);
	}
	/**
	 * Initializes internal fields for new query. This method must be called before calling
	 * the method <code>approximateParetoSet</code>.
	 * 
	 * @param query				the query to optimize
	 * @param consideredMetrics	Boolean flags indicating for each metric if it is considered
	 * @param planSpace			determines the set of scan and join operators to optimize
	 * @param costModel			used to calculate cost of query plans
	 */
	protected abstract void init(Query query, boolean[] consideredMetrics, 
			PlanSpace planSpace, MultiCostModel costModel);
	/**
	 * Can be used by sub-classes to clear algorithm-specific internal data structures.
	 * This cannot free the current approximation however as it is returned as result!
	 */
	public abstract void cleanUp();
}
