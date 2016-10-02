package optimizer.parallelized.partitioning;

import cost.CostModel;
import optimizer.parallelized.SlaveTask;
import plans.JoinOrderSpace;
import plans.spaces.PlanSpace;
import queries.Query;

/**
 * Describes a task that must be executed by a slave.
 * 
 * @author immanueltrummer
 *
 */
public class PartitioningSlaveTask extends SlaveTask {
	/**
	 * Used to verify the class version.
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * identifier of current search space partition
	 */
	final int partitionID;
	/**
	 * total number of search space partitions
	 */
	final int nrPartitions;
	
	public PartitioningSlaveTask(Query query, JoinOrderSpace joinOrderSpace,
			PlanSpace planSpace, CostModel costModel, boolean[] consideredMetrics, 
			double alpha, int partitionID, int nrPartitions, long timeoutMillis) {
		super(query, joinOrderSpace, planSpace, costModel, consideredMetrics, alpha, timeoutMillis);
		this.partitionID = partitionID;
		this.nrPartitions = nrPartitions;
	}
}
