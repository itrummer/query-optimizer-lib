package util;

import static common.Constants.*;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import plans.Plan;
import queries.Query;
import relations.Relation;

/**
 * Contains several utility functions related to cost vector and plan comparisons.
 * 
 * @author immanueltrummer
 *
 */
public class PruningUtil {
	/**
	 * Check if the fist cost value approximates the second for given approximation factor.
	 * 
	 * @param c1		first cost value
	 * @param c2		second cost value
	 * @param alpha		approximation factor
	 * @return			Boolean value indicating whether first cost value approximates second
	 */
	public static boolean approximates(double c1, double c2, double alpha) {
		assert(alpha >= 1);
		if (c1 <= c2 * alpha) {
			return true;
		} else {
			return false;
		}
	}
	/**
	 * Checks if the first cost vector approximately dominates the second
	 * for given approximation factor and set of cost metrics to consider.
	 * 
	 * @param v1				first cost vector
	 * @param v2				second cost vector
	 * @param alpha				approximation factor
	 * @param consideredMetric	Boolean flag for each metric indicating whether it is used
	 * @return					Boolean indicating whether first vector approximates second
	 */
	public static boolean approximatelyDominates(
			double[] v1, double[] v2, double alpha, boolean[] consideredMetric) {
		assert(v1.length == NR_COST_METRICS);
		assert(v2.length == NR_COST_METRICS);
		assert(consideredMetric.length == NR_COST_METRICS);
		for (int metricCtr=0; metricCtr<NR_COST_METRICS; ++metricCtr) {
			if (consideredMetric[metricCtr]) {
				// If first vector does not approximately dominate second vector even
				// for one relevant cost metric then it cannot approximate the second.
				if (!approximates(v1[metricCtr], v2[metricCtr], alpha)) {
					return false;
				}
			}
		}
		// If we arrive here then the first vector approximates the second 
		// for all relevant metrics.
		return true;
	}
	/**
	 * Checks if the first cost vector is at least as good as the second one in each cost metric
	 * and better in at least one, considering only the specified cost metrics.
	 * 
	 * @param v1				first cost vector
	 * @param v2				second cost vector
	 * @param consideredMetric	Boolean flag for each metric indicating whether it is used
	 * @return					Boolean indicating if first vector strictly dominates second
	 */
	public static boolean ParetoDominates(double[] v1, double[] v2, boolean[] consideredMetric) {
		assert(v1.length == NR_COST_METRICS);
		assert(v2.length == NR_COST_METRICS);
		assert(consideredMetric.length == NR_COST_METRICS);
		boolean betterInOne = false;
		boolean worseInOne = false;
		for (int metricCtr=0; metricCtr<NR_COST_METRICS; ++metricCtr) {
			if (consideredMetric[metricCtr]) {
				if (v1[metricCtr] < v2[metricCtr]) {
					betterInOne = true;
				} else if (v1[metricCtr] > v2[metricCtr]) {
					worseInOne = true;
				}				
			}
		}
		return betterInOne && !worseInOne;
	}
	/**
	 * Check if two plans producing the same intermediate result 
	 * generate the output in the same form.
	 * 
	 * @param plan1	first query plan
	 * @param plan2	second query plan
	 * @return		Boolean indicating if both plans produce data in the same format
	 */
	public static boolean sameOutputProperties(Plan plan1, Plan plan2) {
		assert(TestUtil.joinSameTables(plan1, plan2));
		// If one plan materializes its output then this might speed up future operations
		if (plan1.materializes != plan2.materializes) {
			return false;
		}
		// If all prior checks were passed then the plans are comparable
		return true;
	}
	/**
	 * Prune plans based on cost values alone (not considering output properties).
	 * This is appropriate when comparing complete plans since a higher cost cannot
	 * be made up for by producing data in a format speeding up the next operations.
	 * A new plan is inserted into the old plans and dominated plans are pruned out.
	 * 
	 * @param oldPlans			set of Pareto-optimal query plans
	 * @param newPlan			one new plan not contained in the old plans
	 * @param consideredMetric	Boolean flags indicating which metrics to consider
	 */
	public static void pruneCostBased(
			List<Plan> oldPlans, Plan newPlan, boolean[] consideredMetric) {
		// Check if new plan dominated
		for (Plan oldPlan : oldPlans) {
			if (approximatelyDominates(oldPlan.getCostValuesCopy(), newPlan.getCostValuesCopy(), 1, consideredMetric)) {
				//return oldPlans;
				return;
			}
		}
		// If we arrive here then the new plan will definitely be inserted.
		// Prune old plans dominated by new one
		Iterator<Plan> oldPlansIter = oldPlans.iterator();
		while (oldPlansIter.hasNext()) {
			Plan oldPlan = oldPlansIter.next();
			if (approximatelyDominates(newPlan.getCostValuesCopy(), oldPlan.getCostValuesCopy(), 1, consideredMetric)) {
				oldPlansIter.remove();
			}
		}
		oldPlans.add(newPlan);
	}	
	/**
	 * Prune plans producing the same relation using their cost and output properties.
	 * This function changes the list of Pareto plans that is associated with the given relation.
	 * One new plan is inserted if it is not approximately dominated by another plan whose
	 * output has the same properties.
	 * 
	 * @param query				we compare partial plans for that query
	 * @param rel				a relation 
	 * @param newPlan			a new plan producing the given relation
	 * @param alpha				approximation factor; less plans are kept with a higher alpha 
	 * @param consideredMetric	Boolean flags indicating which metrics to consider
	 * @param insertCopy		whether to insert the given plan as Pareto plan or a copy of it
	 */
	public static void prune(Query query, Relation rel, Plan newPlan, double alpha,
			boolean[] consideredMetric, boolean insertCopy) {
		// Make sure that Pareto plan list is initialized
		if (rel.ParetoPlans == null) {
			rel.ParetoPlans = new LinkedList<Plan>();
		}
		// Check if there are similar plans to the new plan and return in that case
		double[] newCost = newPlan.cost;
		for (Plan oldPlan : rel.ParetoPlans) {
			if (PruningUtil.sameOutputProperties(newPlan, oldPlan) && 
					PruningUtil.approximatelyDominates(
							oldPlan.cost, newCost, alpha, consideredMetric)) {
				return;
			}
		}
		// New plan will be inserted - prune prior plans with precise comparisons
		Iterator<Plan> planIter = rel.ParetoPlans.iterator();
		while (planIter.hasNext()) {
			Plan oldPlan = planIter.next();
			if (PruningUtil.sameOutputProperties(newPlan, oldPlan) &&
					PruningUtil.approximatelyDominates(
							newCost, oldPlan.cost, 1, consideredMetric)) {
				planIter.remove();
			}
		}
		if (insertCopy) {
			newPlan = newPlan.deepMutableCopy();
		}
		if (SAFE_MODE) {
			newPlan.makeImmutable();			
		}
		rel.ParetoPlans.add(newPlan);
	}
}
