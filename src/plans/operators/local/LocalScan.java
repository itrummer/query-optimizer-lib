package plans.operators.local;

import plans.operators.ScanOperator;

/**
 * Common super class of all local (i.e., executed on a single node) scan operators.
 * 
 * @author immanueltrummer
 *
 */
public class LocalScan extends ScanOperator{
	/**
	 * Used to verify the class version.
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String toString() {
		return "SCAN";
	}

	@Override
	public LocalScan deepCopy() {
		return new LocalScan();
	}
}
