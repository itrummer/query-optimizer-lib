package queries;

import static common.RandomNumbers.*;

import java.util.Arrays;

/**
 * Produces queries whose cardinalities and selectivities are selected randomly.
 * The factory can be configured via various options allowing to compare algorithms
 * in different scenarios.
 * 
 * @author immanueltrummer
 *
 */
public class QueryFactory {
	/**
	 * Calculate selectivities between two tables based on the domain sizes of the join columns and the table cardinalities.
	 * @param domain1Size	number of distinct values in join column 1
	 * @param domain2Size	number of distinct values in join column 2
	 * @return				the estimated selectivity for an equality join
	 */
	static double calculateSelectivity(double domain1Size, double domain2Size, 
			double cardinality1, double cardinality2, JoinType joinType) {
		switch (joinType) {
		case RANDOM:
			return random.nextDouble();
		case MIN:
			return 1.0/Math.max(cardinality1, cardinality2);
		case MAX:
			return 1.0/Math.min(cardinality1, cardinality2);
		case MN:
			/* Steinbrunn refers to the paper by Selinger et al. '79 who specified
			 * the formula that is used in the following (using the maximum) - the
			 * formula 1.0/min(domain1Size, domain2Size) was specified by Steinbrunn 
			 * which seems however to be a typo (the paper by Bruno from 2010 
			 * supports that interpretation).
			 */
			return 1.0/Math.max(domain1Size, domain2Size);
		case MINMAX:
			double lb = 1.0/Math.max(cardinality1, cardinality2);
			double ub = 1.0/Math.min(cardinality1, cardinality2);
			assert(lb <= ub);
			double delta = ub - lb;
			return lb + random.nextDouble() * delta;
		default:
			assert(false);
			return -1;
		}
	}
	/**
	 * Produce queries in a similar way as Steinbrunn et al. (VLDBJ, 1997: "Heuristic and 
	 * randomized optimization for the join ordering problem") and Bruno (ICDE, 2010:
	 * "Polynomial heuristics for query optimization").
	 * 
	 * @param joinGraphType		structure of join graph
	 * @param nrTables			number of joined tables
	 * @param maxCardinality	maximal cardinality of base tables
	 * @return					a query object that was generated randomly 
	 * 							under the given constraints 
	 */
	public static Query produce(JoinGraphType joinGraphType, 
			int nrTables, double maxCardinality, JoinType joinType) {
		assert(maxCardinality >= 1);
		// calculate scaling factor
		double scaling = maxCardinality / 100000.0;
		// choose cardinalities and domain sizes
		double[] cardinalities = new double[nrTables];
		double[] domainSizes = new double[nrTables];
		for (int tableCtr = 0; tableCtr < nrTables; tableCtr++) {
			/*
			double r = random.nextDouble();
			if (r<0.15) {
				cardinalities[tableCtr] = random.nextInt(91)+10;
				domainSizes[tableCtr] = random.nextInt(9) + 2;
			} else if (r<0.45) {
				cardinalities[tableCtr] = random.nextInt(901)+100;
				domainSizes[tableCtr] = random.nextInt(91) + 10;
			} else if (r<0.8) {
				cardinalities[tableCtr] = random.nextInt(9001)+1000;
				domainSizes[tableCtr] = random.nextInt(401) + 100;
			} else {
				cardinalities[tableCtr] = random.nextInt(90001) + 10000;
				domainSizes[tableCtr] = random.nextInt(501) + 500;
			}
			*/
			// randomly select table cardinality
			double r = random.nextDouble();
			if (r<0.15) {
				cardinalities[tableCtr] = random.nextInt(91)+10;
			} else if (r<0.45) {
				cardinalities[tableCtr] = random.nextInt(901)+100;
			} else if (r<0.8) {
				cardinalities[tableCtr] = random.nextInt(9001)+1000;
			} else {
				cardinalities[tableCtr] = random.nextInt(90001) + 10000;
			}
			// randomly select domain size
			r = random.nextDouble();
			if (r<0.05) {
				domainSizes[tableCtr] = random.nextInt(9) + 2;
			} else if (r<0.55) {
				domainSizes[tableCtr] = random.nextInt(91) + 10;
			} else if (r<0.85) {
				domainSizes[tableCtr] = random.nextInt(401) + 100;
			} else {
				domainSizes[tableCtr] = random.nextInt(501) + 500;
			}
		}
		// scale cardinalities and domain sizes
		for (int tableCtr=0; tableCtr<nrTables; tableCtr++) {
			cardinalities[tableCtr] *= scaling;
			cardinalities[tableCtr] = Math.max(cardinalities[tableCtr], 1);
			//domainSizes[tableCtr] *= scaling;
		}
		// initialize selectivity matrix
		double[][] selectivityMatrix = new double[nrTables][nrTables];
		for (int i=0; i<nrTables; ++i) {
			Arrays.fill(selectivityMatrix[i], 1.0);
		}
		// Calculate number of predicates
		int nrPredicates = -1;
		switch (joinGraphType) {
		case CHAIN:
			nrPredicates = nrTables - 1;
			break;
		case CYCLE:
			nrPredicates = nrTables;
			break;
		case STAR:
			nrPredicates = nrTables - 1;
			break;
		default:
			break;
		}
		assert(nrPredicates >= 0);
		// Calculate predicate selectivities
		switch (joinGraphType) {
			case STAR:
				for (int predicateIndex=0; predicateIndex<nrPredicates; ++predicateIndex) {
					int dimTableIndex = predicateIndex + 1;
					double domain1Size = domainSizes[0];
					double domain2Size = domainSizes[dimTableIndex];
					double cardinality1 = cardinalities[0];
					double cardinality2 = cardinalities[dimTableIndex];
					double selectivity = calculateSelectivity(
							domain1Size, domain2Size, cardinality1, cardinality2, joinType);
					selectivityMatrix[0][dimTableIndex] = selectivity;
					selectivityMatrix[dimTableIndex][0] = selectivity;
				}
				break;
			case CHAIN:
				for (int predicateIndex=0; predicateIndex<nrPredicates; ++predicateIndex) {
					int table1Index = predicateIndex;
					int table2Index = table1Index + 1;
					double domain1Size = domainSizes[table1Index];
					double domain2Size = domainSizes[table2Index];
					double cardinality1 = cardinalities[table1Index];
					double cardinality2 = cardinalities[table2Index];
					double selectivity = calculateSelectivity(
							domain1Size, domain2Size, cardinality1, cardinality2, joinType);
					selectivityMatrix[table1Index][table2Index] = selectivity;
					selectivityMatrix[table2Index][table1Index] = selectivity;
				}
				break;
			case CYCLE:
				for (int predicateIndex=0; predicateIndex<nrPredicates-1; ++predicateIndex) {
					int table1Index = predicateIndex;
					int table2Index = table1Index + 1;
					double domain1Size = domainSizes[table1Index];
					double domain2Size = domainSizes[table2Index];
					double cardinality1 = cardinalities[table1Index];
					double cardinality2 = cardinalities[table2Index];
					double selectivity = calculateSelectivity(
							domain1Size, domain2Size, cardinality1, cardinality2, joinType);
					selectivityMatrix[table1Index][table2Index] = selectivity;
					selectivityMatrix[table2Index][table1Index] = selectivity;
				}
				// connect table with index 0 to table with highest index
				double domain1Size = domainSizes[0];
				double domain2Size = domainSizes[nrTables-1];
				double cardinality1 = cardinalities[0];
				double cardinality2 = cardinalities[nrTables-1];
				double selectivity = calculateSelectivity(
						domain1Size, domain2Size, cardinality1, cardinality2, joinType);
				selectivityMatrix[0][nrTables-1] = selectivity;
				selectivityMatrix[nrTables-1][0] = selectivity;
				break;
			default:
				selectivityMatrix = null;
		}
		assert(selectivityMatrix != null);
		// construct query
		return new Query(nrTables, cardinalities, selectivityMatrix);
	}
	/**
	 * Produces a query with default cardinality range as in the original Steinbrunn paper.
	 * 
	 * @param joinGraph	join graph structure
	 * @param nrTables	number of query tables
	 * @return			a randomly generated query under the given constraints
	 */
	public static Query produceSteinbrunn(JoinGraphType joinGraph, int nrTables, JoinType joinType) {
		return produce(joinGraph, nrTables, 100000, joinType);
	}
}
