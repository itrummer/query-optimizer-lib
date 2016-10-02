package plans.operators.local;

/**
 * Represents a block-nested loop join: this join operator implementation reads chunks
 * of data for the left operand and scans the right operand for each such chunk, checking
 * for each pair of tuples from the left and right operand whether it satisfies the join
 * predicate. Block-nested loop joins can be pipelined meaning that the left operand is
 * not read from disc but from main memory. We can decide whether the join result should
 * be written to disc or left in main memory (allowing to pipeline the following joins).
 * 
 * @author immanueltrummer
 *
 */
public class BNLjoin extends LocalJoin {
	/**
	 * Used to verify the class version.
	 */
	private static final long serialVersionUID = 1L;
	
	public BNLjoin(double buffer, boolean materializeResult) {
		super(buffer, materializeResult);
	}
	@Override
	public String toString() {
		return "BNL<" + buffer + "," + materializeResult + ">";
	}
	@Override
	public BNLjoin deepCopy() {
		return new BNLjoin(buffer, materializeResult);
	}
}
