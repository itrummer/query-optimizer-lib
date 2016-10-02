package plans.operators;

/**
 * Represents an implementation for a join operation
 * 
 * @author immanueltrummer
 *
 */
public abstract class JoinOperator extends Operator {
	/**
	 * Used to check the class version
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * Whether the join output is materialized to disc.
	 */
	public final boolean materializeResult;
	
	public JoinOperator(boolean materializeResult) {
		this.materializeResult = materializeResult;
	}
}
