package plans.operators;

import java.io.Serializable;

/**
 * Represents a scan or join operator used in a query plan.
 * 
 * @author immanueltrummer
 *
 */
public abstract class Operator implements Serializable {
	/**
	 * Used to check the class version.
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * Make deep copy of operator.
	 * 
	 * @return	a deep copy of this operator
	 */
	public abstract Operator deepCopy();
}
