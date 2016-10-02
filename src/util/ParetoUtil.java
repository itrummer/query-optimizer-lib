package util;

import plans.Plan;

import java.util.List;

/**
 * Utility functions for comparing Pareto plan frontier approximations.
 * 
 * @author immanueltrummer
 *
 */
public class ParetoUtil {
	/**
	 * Calculates the epsilon error when trying to approximate the reference vector by the
	 * tested vector. The epsilon error is the minimal value such that scaling the reference
	 * cost vector up by (1 + epsilon) makes the tested vector dominate the reference vector.
	 * 
	 * @param testedVector		epsilon error captures how well this vector approximates reference
	 * @param referenceVector	the reference vector that must be approximated
	 * @param consideredMetrics	Boolean flags indicating for each cost metric if it is considered
	 * @return					the relative error when approximating reference by test vector
	 */
	public static double epsilonError(double[] testedVector, 
			double[] referenceVector, boolean[] consideredMetrics) {
		assert(testedVector.length == referenceVector.length);
		int nrMetrics = testedVector.length;
		// Calculate error as maximum over all relevant cost metrics
		double maxError = 0;
		for (int metricCtr=0; metricCtr<nrMetrics; ++metricCtr) {
			if (consideredMetrics[metricCtr]) {
				// Error is zero if tested vector is better according to current metric
				double testCost = testedVector[metricCtr];
				double refCost = referenceVector[metricCtr];
				double error = Double.NaN;
				if (refCost == 0) {
					if (testCost == 0) {
						error = 0;
					} else {
						error = Double.POSITIVE_INFINITY;
					}
				} else {
					error = Math.max(testCost / refCost - 1, 0);
				}
				assert(!Double.isNaN(error)) : 
					"Metric: " + metricCtr + "Test cost: " + testCost + "; Ref cost: " + refCost;
				maxError = Math.max(maxError, error);			
			}
		}
		return maxError;
	}
	/**
	 * Calculates the epsilon error when trying to approximate the reference frontier by the
	 * tested frontier. We find for each reference vector the test vector that approximates it
	 * best. Then we use the approximation error for the reference vector whose approximation
	 * is the worst as measure for the overall quality of the tested frontier. This corresponds
	 * to the metric recommended in "Performance assessment of multiobjective optimizers: 
	 * An analysis and review" by Zitzler and Thiele, 2003.
	 * 
	 * @param testedFrontier	set of cost vectors that should approximate the reference vectors
	 * @param referenceFrontier	set of cost vectors that should be approximated
	 * @param consideredMetrics	Boolean flags indicating for each metric if it is relevant
	 * @return					epsilon error capturing how well the reference set is approximated
	 */
	public static double epsilonError(List<Plan> testedFrontier, 
			List<Plan> referenceFrontier, boolean[] consideredMetrics) {
		// Calculate total error as maximum over all plans in the reference set
		double setError = 0;
		for (Plan referencePlan : referenceFrontier) {
			// Calculate reference plan error as minimum over all plans in the tested set
			double planError = Double.POSITIVE_INFINITY;
			for (Plan testPlan : testedFrontier) {
				double planPairError = epsilonError(testPlan.getCostValuesCopy(), 
						referencePlan.getCostValuesCopy(), consideredMetrics);
				planError = Math.min(planError, planPairError);
			}
			setError = Math.max(setError, planError);
		}
		return setError;
	}
}
