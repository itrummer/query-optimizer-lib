package plans.operators.local;

import plans.operators.JoinOperator;

/**
 * Common super class of all local (i.e., those joins operators are meant for execution
 * on a single node) join operator implementations.
 * 
 * @author immanueltrummer
 *
 */
public abstract class LocalJoin extends JoinOperator {
	/**
	 * Used to verify the class version.
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * The amount of buffer space reserved for the operator, measured in bytes.
	 */
	public final double buffer;
	
	public LocalJoin(double buffer, boolean materializeResult) {
		super(materializeResult);
		assert(buffer>=0);
		this.buffer = buffer;
	}
}
