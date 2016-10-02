package queries;

/**
 * The join type decides how the selectivity is randomly chosen.
 * RANDOM	selectivity is chosen randomly with uniform distribution
 * MN		selectivity is chosen assuming an equality join
 * MIN		selectivity is chosen assuming foreign key constraint from smaller to larger table
 * MAX		selectivity is chosen assuming foreign key constraint from larger to smaller table
 * MINMAX	selectivity is chosen somewhere in between the MIN and the MAX selectivity
 * 
 * @author itrummer
 *
 */
public enum JoinType {
	RANDOM, MN, MIN, MAX, MINMAX
}
