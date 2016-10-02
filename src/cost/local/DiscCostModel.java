package cost.local;

import cost.SingleCostModel;
import plans.JoinPlan;
import plans.Plan;
import plans.ScanPlan;
import plans.operators.JoinOperator;
import plans.operators.ScanOperator;
import plans.operators.local.LocalJoin;
import plans.operators.local.LocalScan;

/**
 * Calculates the amount of disc space (e.g., for materializing intermediate results)
 * that is consumed by a query plan.
 * 
 * @author immanueltrummer
 *
 */
public class DiscCostModel extends SingleCostModel {
	/**
	 * Used to verify the class version.
	 */
	private static final long serialVersionUID = 1L;

	public DiscCostModel(int metricIndex) {
		super(metricIndex);
	}
	
	@Override
	protected void updateScanRoot(ScanPlan plan) {
		plan.setCostValue(metricIndex, -1);
		ScanOperator scanOperator = plan.scanOperator;
		if (scanOperator instanceof LocalScan) {
			plan.setCostValue(metricIndex, plan.outputPages);
		}
		assert(plan.getCostValue(metricIndex)>=0);
	}

	@Override
	protected void updateJoinRoot(JoinPlan plan) {
		plan.setCostValue(metricIndex, -1);
		JoinOperator genericJoin = plan.getJoinOperator();
		Plan leftPlan = plan.getLeftPlan();
		Plan rightPlan = plan.getRightPlan();
		if (genericJoin instanceof LocalJoin) {
			LocalJoin join = (LocalJoin)genericJoin;
			// We assume that disc space is not re-used during the execution of the same query plan.
			// Therefore the total disc space consumption is the sum over the disc consumptions of
			// individual operations.
			double leftDisc = leftPlan.getCostValue(metricIndex);
			double rightDisc = rightPlan.getCostValue(metricIndex);
			double addedDisc = join.materializeResult ? plan.outputPages : 0;
			plan.setCostValue(metricIndex, leftDisc + rightDisc + addedDisc);
		}
		assert(plan.getCostValue(metricIndex)>=0);
	}
	
	@Override
	public String toString() {
		return "Disc";
	}

}
