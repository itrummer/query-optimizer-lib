package plans.operators.cluster;

import plans.operators.ScanOperator;

public class ClusterScan extends ScanOperator {

	@Override
	public ClusterScan deepCopy() {
		return new ClusterScan();
	}

}
