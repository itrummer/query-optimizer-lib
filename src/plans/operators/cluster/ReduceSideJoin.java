package plans.operators.cluster;

import plans.operators.JoinOperator;

public class ReduceSideJoin extends JoinOperator {
	/**
	 * Indicates whether the two inputs are generated in parallel or sequentially.
	 */
	public final boolean parallelized;
	/**
	 * How many machines (mappers and reducers) are used for this join?
	 */
	public final int nrMachines;
	
	public ReduceSideJoin(boolean parallelized, int nrMachines) {
		super(true);
		this.parallelized = parallelized;
		this.nrMachines = nrMachines;
	}

	@Override
	public ReduceSideJoin deepCopy() {
		return new ReduceSideJoin(parallelized, nrMachines);
	}
}
