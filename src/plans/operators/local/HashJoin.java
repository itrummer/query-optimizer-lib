package plans.operators.local;

/**
 * Represents the hash join operator implementation: this implementation builds a hash
 * table on one of the join input operands and uses it to quickly identify matching
 * partners for the tuples from the other join operand.
 * 
 * @author immanueltrummer
 *
 */
public class HashJoin extends LocalJoin {
	/**
	 * Used to verify the class version.
	 */
	private static final long serialVersionUID = 1L;
	
	public HashJoin(double buffer, boolean materializeResult) {
		super(buffer, materializeResult);
	}
	@Override
	public String toString() {
		return "HSJ<" + buffer + "," + materializeResult + ">";
	}
	@Override
	public HashJoin deepCopy() {
		return new HashJoin(buffer, materializeResult);
	}
}
