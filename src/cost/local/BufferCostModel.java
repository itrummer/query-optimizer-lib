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
 * This cost model estimates the amount of buffer space consumed by a query plan.
 * This metric is particularly important in scenarios where multiple query plans
 * are executed concurrently such that none of them should consume the entire
 * main memory.
 * 
 * @author immanueltrummer
 *
 */
public class BufferCostModel extends SingleCostModel {
	/**
	 * Used to verify the class version.
	 */
	private static final long serialVersionUID = 1L;

	public BufferCostModel(int metricIndex) {
		super(metricIndex);
	}
	
	@Override
	protected void updateScanRoot(ScanPlan plan) {
		plan.setCostValue(metricIndex, -1);
		ScanOperator scanOperator = plan.scanOperator;
		if (scanOperator instanceof LocalScan) {
			plan.setCostValue(metricIndex, 0);
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
			// We conservatively assume that buffer space consumption is added between the left and right
			// sub-plan and the top-level join operator. After materialization the buffer consumption
			// reduces however to zero. In those cases we take into account the buffer consumption before
			// and after materialization and take the maximum (-> we represent the maximal buffer consumption
			// that occured over the whole execution of the query plan).
			boolean leftMaterializes = leftPlan.materializes;
			boolean rightMaterializes = rightPlan.materializes;
			double leftBuffer = leftPlan.getCostValue(metricIndex);
			double rightBuffer = rightPlan.getCostValue(metricIndex);
			double joinBuffer = join.buffer;
			if (!leftMaterializes && !rightMaterializes) {
				plan.setCostValue(metricIndex, leftBuffer + rightBuffer + joinBuffer);
			} else if (leftMaterializes && !rightMaterializes) {
				plan.setCostValue(metricIndex, Math.max(leftBuffer, rightBuffer + joinBuffer));
			} else if (!leftMaterializes && rightMaterializes) {
				plan.setCostValue(metricIndex, Math.max(rightBuffer, leftBuffer + joinBuffer));
			} else {
				assert(leftMaterializes && rightMaterializes);
				plan.setCostValue(metricIndex, Math.max(leftBuffer, Math.max(rightBuffer, joinBuffer)));
			}
		}
		assert(plan.getCostValue(metricIndex)>=0);
	}
	
	@Override
	public String toString() {
		return "Buffer";
	}

}
