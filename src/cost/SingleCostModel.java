package cost;

import plans.JoinPlan;
import plans.Plan;
import plans.ScanPlan;

// Abstract class for calculating cost according to one single cost metric.
public abstract class SingleCostModel extends CostModel {
	// Index of that metric within each cost vector
	protected final int metricIndex;
	public SingleCostModel(int metricIndex) {
		this.metricIndex = metricIndex;
	}
	// Updates cost of this metric at the root plan node. Assumes that the
	// cost of sub-plans (if any) for this metric has been calculated before.
	@Override
	public void updateRoot(Plan plan) {
		if (plan instanceof ScanPlan) {
			updateScanRoot((ScanPlan)plan);
		} else {
			assert(plan instanceof JoinPlan);
			updateJoinRoot((JoinPlan)plan);
		}
	}
	// Updates plan root in case it is a scan plan.
	protected abstract void updateScanRoot(ScanPlan plan);
	// Updates plan root in case it is a join plan.
	protected abstract void updateJoinRoot(JoinPlan plan);
}
