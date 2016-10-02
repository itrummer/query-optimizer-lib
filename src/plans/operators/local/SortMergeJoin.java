package plans.operators.local;

/**
 * Represents sort-merge join operator implementation: the sort-merge join operator sorts
 * its inputs on the join column (we assume an equality join) and merges the sorted inputs.
 * 
 * @author immanueltrummer
 *
 */
public class SortMergeJoin extends LocalJoin {
	/**
	 * Used to verify the class version.
	 */
	private static final long serialVersionUID = 1L;
	
	public SortMergeJoin(double buffer, boolean materializeResult) {
		super(buffer, materializeResult);
	}
	@Override
	public String toString() {
		return "SMJ<" + buffer + "," + materializeResult + ">";
	}
	@Override
	public SortMergeJoin deepCopy() {
		return new SortMergeJoin(buffer, materializeResult);
	}
}
