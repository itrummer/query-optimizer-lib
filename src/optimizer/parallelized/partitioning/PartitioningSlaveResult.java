package optimizer.parallelized.partitioning;

import java.util.List;

import optimizer.parallelized.SlaveResult;
import plans.Plan;

public class PartitioningSlaveResult extends SlaveResult {
	/**
	 * Used to verify the class version.
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * Contains the Pareto-optimal plans found in the plan space partition dedicate
	 * to the corresponding worker node.
	 */
	public final List<Plan> paretoPlans;

	public PartitioningSlaveResult(List<Plan> paretoPlans, long mainMemoryConsumption,
			long elapsedMillis, boolean timeout, boolean memoryOut, String errors) {
		super(mainMemoryConsumption, elapsedMillis, timeout, memoryOut, errors);
		this.paretoPlans = paretoPlans;
	}

}
