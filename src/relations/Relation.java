package relations;

import java.io.Serializable;
import java.util.BitSet;
import java.util.List;

import common.Constants;
import plans.Plan;
 
/**
 * Represents a base table or a join between several tables.
 * 
 * @author immanueltrummer
 *
 */
public class Relation implements Serializable {
	/**
	 * Used to check the class version.
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * Indices of the tables that are joined in this relation.
	 */
	public final BitSet tableSet;
	/**
	 * The number of rows of this relation.
	 */
	public final double cardinality;
	/**
	 * The number of disc pages consumed by this relation.
	 */
	public final double pages;
	/**
	 * The Pareto-optimal plans for creating this relation - this field
	 * is not used by all optimization algorithms.
	 */
	public List<Plan> ParetoPlans;
	/**
	 * How often was this relation considered? This field is used by algorithms that
	 * associate relations with arms in a multi-armed bandit scenario.
	 */
	public long nrRounds = 1;
	/**
	 * How often was this relation selected? This field is used by algorithms that
	 * associate relations with arms in a multi-armed bandit scenario.
	 */
	public long nrPlayed = 1;
	/**
	 * What was the accumulated reward acquired by selecting this relation? This field is used by
	 * algorithms that associate relations with arms in a multi-armed bandit scenario.
	 */
	public double accumulatedReward;
	/**
	 * What is the UCB value of this relation derived from the number of times it was considered and
	 * played and from the accumulated reward achieved by "playing" this relation.
	 */
	public double UCBvalue;
	
	public Relation(BitSet tableSet, double cardinality) {
		this.tableSet = tableSet;
		this.cardinality = cardinality;
		this.pages = Math.ceil(cardinality * Constants.BYTES_PER_TUPLE/Constants.BYTES_PER_PAGE);
	}
	/**
	 * Obtains index of first table joined in this relation.
	 * 
	 * @return	the index of a table that is joined by this relation
	 */
	public int firstTableIndex() {
		return tableSet.nextSetBit(0);
	}
}
