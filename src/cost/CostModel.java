package cost;

import java.io.Serializable;

import plans.JoinPlan;
import plans.Plan;
import plans.ScanPlan;

/**
 * This class calculates the execution cost of query plans for one or several cost metrics.
 * 
 * @author immanueltrummer
 *
 */
public abstract class CostModel implements Serializable {
	/**
	 * Used to verify the class version.
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * Updates the cost of the root of the plan tree, assuming that the cost
	 * for the other tree nodes has already been calculated.
	 * 
	 * @param plan	the plan for whose root node the cost must be calculated
	 */
	public abstract void updateRoot(Plan plan);
	/**
	 * Update cost of plan root node based on cost of its children and update statistics
	 * 
	 * @param plan	the plan whose root cost must be updated
	 */
	public void updateRootAndStatistics(Plan plan) {
		updateRoot(plan);
	}
	/**
	 * Updates the cost of all plan tree nodes in bottom-up order.
	 * 
	 * @param plan	the plan for which the cost values must be updated
	 */
	public void updateAll(Plan plan) {
		if (plan instanceof ScanPlan) {
			updateRootAndStatistics(plan);
		} else {
			assert(plan instanceof JoinPlan);
			JoinPlan joinPlan = (JoinPlan)plan;
			updateAll(joinPlan.getLeftPlan());
			updateAll(joinPlan.getRightPlan());
			updateRootAndStatistics(joinPlan);
		}
	}
}
