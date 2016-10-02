package optimizer.parallelized.partitioning;

/**
 * Describes a constraint on the join order between two tables: either no constraint or
 * table q needs to appear before table r on the path, or vice-versa.
 * 
 * @author immanueltrummer
 *
 */
public enum ConstraintType {
	NO_CONSTRAINT, Q_PRECEDES_R, R_PRECEDES_Q
}
