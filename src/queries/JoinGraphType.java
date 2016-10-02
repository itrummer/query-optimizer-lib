package queries;

/**
 * Join graph structure to generate by query factory. The names refer to the ones
 * presented in "Heuristic and randomized optimization for the join ordering problem" 
 * by Steinbrunn et al.
 * CHAIN: 	all tables are connected by a chain of join predicates;
 * 			each table except the two ends of the chain are connected to two other tables
 * CYCLE: 	all tables are connected by a series of join predicates forming a cycle;
 * 			each table is connected to two other tables
 * STAR:	one table is connected via one join predicate to each of the other tables
 * 
 * @author immanueltrummer
 *
 */
public enum JoinGraphType {
	CHAIN, CYCLE, STAR
}
