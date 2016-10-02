package optimizer.parallelized.partitioning;

import static optimizer.parallelized.partitioning.ConstraintType.*;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import cost.CostModel;
import optimizer.approximate.BitSetIterator;
import optimizer.parallelized.Slave;
import plans.JoinOrderSpace;
import plans.JoinPlan;
import plans.Plan;
import plans.ScanPlan;
import plans.operators.JoinOperator;
import plans.operators.ScanOperator;
import plans.spaces.PlanSpace;
import queries.Query;
import relations.Relation;
import relations.RelationFactory;
import util.MathUtil;
import util.PruningUtil;

/**
 * Each slave searches the best plan(s) in one specific plan space partition. 
 * 
 * @author immanueltrummer
 *
 */
public class PartitioningSlave extends Slave {
	/**
	 * Extracts constraint type from binary constraint vector, table subset index, and constraint number.
	 * 
	 * @param subsetIndex		index of considered table subset (pair/triple for linear/bushy space)
	 * @param nrConstraints		number of constraints defining search space partition
	 * @param constraintVector	binary vector encoding the partition constraints
	 * @return					a constraint type defining the precedence constraint
	 */
	static ConstraintType readConstraint(int subsetIndex, int nrConstraints, boolean[] constraintVector) {
		// Read constraint
		ConstraintType constraintType = null;
		if (subsetIndex >= nrConstraints) {
			// Number of constraints is insufficient to restrict current table subset
			constraintType = ConstraintType.NO_CONSTRAINT;
		} else {
			constraintType = constraintVector[subsetIndex] ?
					ConstraintType.Q_PRECEDES_R : ConstraintType.R_PRECEDES_Q;
		}
		assert(constraintType != null);
		return constraintType;
	}
	/**
	 * Generates all relevant table sets for a constrained linear plan space partition.
	 * 
	 * @param nrTables			number of query tables to join
	 * @param nrConstraints		number of constraints describing the partition
	 * @param constraintVector	binary vector capturing the nature of each constraint
	 * @return					a set of table sets that satisfy the given constraints
	 */
	static List<BitSet> constraintTableSetsLinear(int nrTables, int nrConstraints, boolean[] constraintVector) {
		assert(nrTables >= 2 * nrConstraints) : "nrTables: " + nrTables + "; nrConstraints: " + nrConstraints;
		assert(constraintVector.length == nrConstraints);
		assert(nrTables % 2 == 0) : nrTables;
		// Will contain all relevant result table sets
		List<BitSet> resultTableSets = new LinkedList<BitSet>();
		resultTableSets.add(new BitSet());
		// Iterate over table pairs
		for (int pairIndex=0; pairIndex<nrTables/2; ++pairIndex) {
			// Read constraint for current table pair
			ConstraintType constraintType = readConstraint(pairIndex, nrConstraints, constraintVector);
			// Generate new table sets by extending old sets
			List<BitSet> newResultTableSets = new LinkedList<BitSet>();
			for (BitSet oldBitset : resultTableSets) {
				// Calculate indices of concerned tables
				int qIndex = pairIndex * 2;
				int rIndex = pairIndex * 2 + 1;
				// Adding both tables is in each case admissible
				BitSet addedBoth = new BitSet();
				addedBoth.or(oldBitset);
				addedBoth.set(qIndex);
				addedBoth.set(rIndex);
				newResultTableSets.add(addedBoth);
				// Adding q but not r is admissible if q precedes r or no constraint
				if (constraintType == NO_CONSTRAINT || constraintType == Q_PRECEDES_R) {
					BitSet addQnotR = new BitSet();
					addQnotR.or(oldBitset);
					addQnotR.set(qIndex);
					newResultTableSets.add(addQnotR);
				}
				// Adding r but not q is admissible if r precedes q or no constraint
				if (constraintType == NO_CONSTRAINT || constraintType == R_PRECEDES_Q) {
					BitSet addRnotQ = new BitSet();
					addRnotQ.or(oldBitset);
					addRnotQ.set(rIndex);
					newResultTableSets.add(addRnotQ);
				}
			}
			// Add new result table sets to old ones
			resultTableSets.addAll(newResultTableSets);
		} // over table pairs
		return resultTableSets;
	}
	/**
	 * Generates all relevant table sets for a constrained bushy plan space partition.
	 * 
	 * @param nrTables			number of query tables to join
	 * @param nrConstraints		number of constraints describing the partition
	 * @param constraintVector	binary vector capturing the nature of each constraint
	 * @return					a set of table sets that satisfy the given constraints
	 */
	static List<BitSet> constraintTableSetsBushy(int nrTables, int nrConstraints, boolean[] constraintVector) {
		assert(nrTables % 3 == 0);
		// Will contain all relevant result table sets
		List<BitSet> resultTableSets = new LinkedList<BitSet>();
		resultTableSets.add(new BitSet());
		// Iterate over table triples
		for (int tripleIndex=0; tripleIndex<nrTables/3; ++tripleIndex) {
			// Read constraint for current table pair
			ConstraintType constraintType = readConstraint(tripleIndex, nrConstraints, constraintVector);
			// Generate new table sets by extending old sets
			List<BitSet> newResultTableSets = new LinkedList<BitSet>();
			for (BitSet oldBitset : resultTableSets) {
				// Calculate indices of concerned tables
				int qIndex = tripleIndex * 3;
				int rIndex = tripleIndex * 3 + 1;
				int sIndex = tripleIndex * 3 + 2;
				// Iterate over whether or not table s is added - if s is not added then
				// no constraint apply on q and r.
				for (boolean addS : new boolean[] {false, true}) {
					// Adding both tables is in each case admissible
					BitSet addedBoth = new BitSet();
					addedBoth.or(oldBitset);
					addedBoth.set(qIndex);
					addedBoth.set(rIndex);
					if (addS) {
						addedBoth.set(sIndex);
					}
					newResultTableSets.add(addedBoth);
					// Adding q but not r is admissible if q precedes r or no constraint
					if (constraintType == NO_CONSTRAINT || constraintType == Q_PRECEDES_R || !addS) {
						BitSet addQnotR = new BitSet();
						addQnotR.or(oldBitset);
						addQnotR.set(qIndex);
						if (addS) {
							addQnotR.set(sIndex);
						}
						newResultTableSets.add(addQnotR);
					}
					// Adding r but not q is admissible if r precedes q or no constraint
					if (constraintType == NO_CONSTRAINT || constraintType == R_PRECEDES_Q || !addS) {
						BitSet addRnotQ = new BitSet();
						addRnotQ.or(oldBitset);
						addRnotQ.set(rIndex);
						if (addS) {
							addRnotQ.set(sIndex);
						}
						newResultTableSets.add(addRnotQ);
					}
				} // Whether S is added or not in the case that at least one of q and r are added
				// Treat case that only s but neither q nor r are added (this is always admissible)
				BitSet addOnlyS = new BitSet();
				addOnlyS.or(oldBitset);
				addOnlyS.set(sIndex);
				newResultTableSets.add(addOnlyS);
			} // Over bit sets generated in prior iterations
			// Add new result table sets to old ones
			resultTableSets.addAll(newResultTableSets);
		} // over table pairs
		return resultTableSets;
	}
	/**
	 * Generate a list mapping cardinality to sets of relevant table sets with that cardinality.
	 * 
	 * @param nrTables			number of tables that the query to optimize joins
	 * @param partitionID		integer ID of the search space partition to optimize
	 * @param nrPartitions		number of search space partitions
	 * @param joinOrderSpace	whether uniquely linear or also bushy query plans are considered
	 * @return					an array containing at each index a list of table sets of the same cardinality
	 */
	static List<List<BitSet>> generateResultTableSets(int nrTables, 
			int partitionID, int nrPartitions, JoinOrderSpace joinOrderSpace) {
		// The number of constraints grows proportional to the logarithm of the number of partitions
		int nrConstraints = (int)MathUtil.logOfBase(2, nrPartitions);
		/* Represent constraints as bit vector: each vector component represents a constraint between
		 * a pair of tables (for linear join order space) or between a triple of tables (bushy space).
		 * If the vector component is true then x < y for linear space or x < y | z for bushy space
		 * where x and y (and additionally z) are the tables that the constraint refers to with 
		 * ascending indices from x to z.
		 */
		boolean[] constraintVector = MathUtil.toBitVector(partitionID, nrConstraints);
		// Will contain all relevant result table sets
		List<BitSet> resultTableSets = null;
		// Table sets are generated differently for linear and bushy join order spaces
		switch (joinOrderSpace) {
		case LINEAR:
			resultTableSets = constraintTableSetsLinear(nrTables, nrConstraints, constraintVector);
			break;
		case BUSHY:
			resultTableSets = constraintTableSetsBushy(nrTables, nrConstraints, constraintVector);
			break;
		default:
			assert(false);
		}
		// Will contain for each cardinality a set of relevant table sets for this search space partition
		List<List<BitSet>> tableSetsBySize = new ArrayList<List<BitSet>>(nrTables);
		// Initialize each cardinality field
		for (int cardinalityIndex=0; cardinalityIndex<nrTables; ++cardinalityIndex) {
			tableSetsBySize.add(new LinkedList<BitSet>());
		}
		// Insert previously generated table sets for the right cardinality index
		for (BitSet tableSet : resultTableSets) {
			int cardinality = tableSet.cardinality();
			if (cardinality > 0) {
				tableSetsBySize.get(cardinality-1).add(tableSet);				
			}
		}
		return tableSetsBySize;
	}
	/**
	 * Tries different plans and operator combinations for a given split of the result table set.
	 * This function generates and inserts the relation corresponding to the result table set 
	 * if this has not yet happened. For two given join operands, it tries all combinations of
	 * Pareto-optimal plans for generating the two join operands and all applicable join operators.
	 * For each combination a new plan is generated that is pruned within the Pareto plan set
	 * associated with the result relation.
	 * 
	 * @param query					the query to optimize
	 * @param relations				maps table sets to the corresponding relations
	 * @param leftTables			the table set defining the left (=outer) join input operand
	 * @param rightTables			the table set defining the right (=inner) join input operand
	 * @param resultTables			the union of both input operand sets
	 * @param planSpace				determines the set of scan and join operators to consider
	 * @param costModel				used for estimating plan execution costs for multiple metrics
	 * @param localAlpha			approximation factor used for pruning
	 * @param consideredMetrics		Boolean flags indicating which plan cost metrics are considered
	 */
	static void tryPlansOperators(Query query, Map<BitSet, Relation> relations, BitSet leftTables, 
			BitSet rightTables, BitSet resultTables, PlanSpace planSpace, CostModel costModel, 
			double localAlpha, boolean[] consideredMetrics) {
		// Operand relations must already exist
		Relation leftRel = relations.get(leftTables);
		Relation rightRel = relations.get(rightTables);
		// Generate result relation if it does not yet exist
		if (!relations.containsKey(resultTables)) {
			Relation resultRel = RelationFactory.createJoinRel(query, leftRel, rightRel);
			relations.put(resultTables, resultRel);
		}
		Relation resultRel = relations.get(resultTables);
		// iterate over (near-)Pareto-optimal plans for left and right relation
		for (Plan leftPlan : leftRel.ParetoPlans) {
			for (Plan rightPlan : rightRel.ParetoPlans) {
				// iterate over all applicable join methods
				for (JoinOperator joinOperator : planSpace.joinOperators(leftPlan, rightPlan)) {
					Plan newPlan = new JoinPlan(resultRel.cardinality, 
							resultRel.pages, leftPlan, rightPlan, joinOperator);
					costModel.updateRoot(newPlan);
					PruningUtil.prune(query, resultRel, newPlan, 
							localAlpha, consideredMetrics, false);
				} // over join operators
			} // over right plan
		} // over left plan
	}
	/**
	 * Checks whether one specific table is suitable as inner join operand based on a constraint set.
	 * This version of the function assumes a linear plan space.
	 * 
	 * @param t					index of table for which we check whether it is suitable as inner operand
	 * @param resultSet			the tables contained in the join result
	 * @param nrConstraints		the number of constraints describing the search space partition
	 * @param constraintVector	a Boolean vector representing the constraints defining the partition
	 * @return					Boolean indicating whether the table is suitable as inner operand
	 */
	static boolean okAsInnerLinear(int t, BitSet resultSet, int nrConstraints, boolean[] constraintVector) {
		assert(t >= 0);
		assert(t/2 <= nrConstraints) : "t: " + t + "; nrConstraints: " + nrConstraints;
		assert(resultSet.get(t));
		assert(nrConstraints == constraintVector.length);
		// Obtain index of table pair
		int pairIndex = t/2;
		// Extract constraint referring to this specific table pair
		ConstraintType constraintType = readConstraint(pairIndex, nrConstraints, constraintVector);
		// We call the first table within each table pair q and the second one r
		boolean tIsQ = (t % 2 == 0);
		// Check which tables of the pair are included in result set
		int qIndex = pairIndex * 2;
		int rIndex = pairIndex * 2 + 1;
		boolean qInResult = resultSet.get(qIndex);
		boolean rInResult = resultSet.get(rIndex);
		// Verify whether t is suitable as inner join operand based on constraint type
		switch (constraintType) {
		case NO_CONSTRAINT:
			return true;
		case Q_PRECEDES_R:
			// q must be joined before r - if r is in the result set then we cannot take out q
			if (rInResult) {
				return !tIsQ;
			} else {
				return true;
			}
		case R_PRECEDES_Q:
			// r must be joined before q - if q is in the result set then we cannot take out r
			if (qInResult) {
				return tIsQ;
			} else {
				return true;
			}
		default:
			assert(false);
			return false;
		}
	}
	/**
	 * Try all splits of a result set into two subsets representing join operands.
	 * This version of the function is specific to the linear join order space and considers
	 * only single tables as outer operand.
	 * 
	 * @param query					the query to optimize
	 * @param relations				maps table sets to the corresponding relations
	 * @param resultTables			the join result table set to split into two subsets 
	 * @param partitionID			ID of plan space partition being optimized within
	 * @param nrPartitions			the total number of join order space partitions
	 * @param planSpace				determines the set of applicable scan and join operators
	 * @param costModel				used to estimate the execution cost of query plans for multiple metrics
	 * @param localAlpha			approximation factor used during pruning
	 * @param consideredMetrics		Boolean flags indicating which plan cost metrics are considered
	 */
	static void trySplitsLinear(Query query, Map<BitSet, Relation> relations, BitSet resultTables, 
			int partitionID, int nrPartitions, PlanSpace planSpace, CostModel costModel,  
			double localAlpha, boolean[] consideredMetrics) {
		// Extract constraints
		int nrConstraints = (int)MathUtil.logOfBase(2, nrPartitions);
		boolean[] constraintVector = MathUtil.toBitVector(partitionID, nrConstraints);
		// Iterate over tables in result set that could become inner operand
		for (int q = resultTables.nextSetBit(0); q >= 0; q = resultTables.nextSetBit(q+1)) {
			if (okAsInnerLinear(q, resultTables, nrConstraints, constraintVector)) {
				// Generate table set describing outer and inner operand
				BitSet leftTables = new BitSet();
				BitSet rightTables = new BitSet();
				leftTables.or(resultTables);
				leftTables.clear(q);
				rightTables.set(q);
				// Generate all plans that correspond to current result set split
				tryPlansOperators(query, relations, leftTables, rightTables, resultTables, 
						planSpace, costModel, localAlpha, consideredMetrics);
			}
		}
	}
	/**
	 * Searches for the specified table in left and right join operand and join result table set.
	 * Returns a character describing whether the table was found and if yes where.
	 * 
	 * @param tableIndex	index of table to search for
	 * @param leftSet		tables assigned as outer/left join operand
	 * @param rightSet		tables assigned as inner/right join operand
	 * @param resultSet		tables in join result set (a superset of the union of left and right set!)
	 * @return				character 'L', 'R', '-', or 'U' meaning that the table was found in the left
	 * 						set, right set, neither in left, right, nor result set, or found only in
	 * 						the result set. 
	 */
	static char find(int tableIndex, BitSet leftSet, BitSet rightSet, BitSet resultSet) {
		assert(resultSet.cardinality() >= leftSet.cardinality() + rightSet.cardinality());
		assert(!leftSet.intersects(rightSet));
		if (!resultSet.get(tableIndex)) {
			return '-';
		} else if (leftSet.get(tableIndex)) {
			return 'L';
		} else if (rightSet.get(tableIndex)) {
			return 'R';
		} else {
			return 'U';
		}
	}
	/**
	 * Contains strings describing the positions of three constrained tables within the current
	 * operand split. The contained strings represent relative positions that do not comply with
	 * the corresponding constraints and must therefore be avoided.
	 */
	final static Map<ConstraintType,HashSet<String>> forbiddenTriples; 
	static {
		forbiddenTriples = new HashMap<ConstraintType,HashSet<String>>();
		// Generate forbidden triples for constraints of the form Q precedes R
		HashSet<String> forbiddenTriplesQprecEqR = new HashSet<String>();
		forbiddenTriplesQprecEqR.add("RLL");
		forbiddenTriplesQprecEqR.add("-LL");
		forbiddenTriplesQprecEqR.add("-RL");
		forbiddenTriplesQprecEqR.add("LRR");
		forbiddenTriplesQprecEqR.add("-RR");
		forbiddenTriplesQprecEqR.add("-LR");
		forbiddenTriples.put(Q_PRECEDES_R, forbiddenTriplesQprecEqR);
		// Generate forbidden triples for constraints of the form R precedes Q
		HashSet<String> forbiddenTriplesRprecEqQ = new HashSet<String>();
		forbiddenTriplesRprecEqQ.add("LRL");
		forbiddenTriplesRprecEqQ.add("L-L");
		forbiddenTriplesRprecEqQ.add("R-L");
		forbiddenTriplesRprecEqQ.add("RLR");
		forbiddenTriplesRprecEqQ.add("R-R");
		forbiddenTriplesRprecEqQ.add("L-R");
		forbiddenTriples.put(R_PRECEDES_Q, forbiddenTriplesRprecEqQ);
		// Generate empty set of forbidden triples for no constraints
		forbiddenTriples.put(NO_CONSTRAINT, new HashSet<String>());
	}
	/**
	 * Verifies whether the given table can be added to outer or inner join operand without
	 * violating the constraints. The method identifies the constraint that applies to the
	 * specified table (at most one constraint can apply to it as all constraints describing
	 * a given search space partition refer to mutually exclusive tables), transforms the
	 * partitioning of the three tables concerned by the constraint into a three-letter
	 * representation and checks whether it is forbidden for the given constraint type.
	 * 
	 * @param t					index of table that should be added to outer/inner operand
	 * @param resultSet			table set that needs to be split into two subsets in an admissible way
	 * @param leftSet			the subset of tables that is currently assigned as outer join operand 
	 * @param rightSet			the subset of tables that is currently assigned as inner join operand
	 * @param nrConstraints		the number of constraints describing search space partition
	 * @param constraintVector	a Boolean vector describing the constraints of this partition
	 * @param newPos			the proposed assignment ('L' or 'R) of the specified table t
	 * @return					Boolean indicating whether the proposed assignment complies is admissible
	 */
	static boolean okAsOuterOrInnerBushy(int t, BitSet resultSet, BitSet leftSet, 
			BitSet rightSet, int nrConstraints, boolean[] constraintVector, char newPos) {
		// Verify that table is assigned either as outer/left or inner/right operand
		assert(newPos == 'L' || newPos == 'R') : newPos;
		// Obtain index of table triple
		int tripleIndex = t/3;
		// Extract constraint referring to this specific triple
		ConstraintType constraintType = readConstraint(tripleIndex, nrConstraints, constraintVector);
		// We call the first table within each triple q, the second r, and the third s
		int qIndex = tripleIndex * 3;
		int rIndex = tripleIndex * 3 + 1;
		int sIndex = tripleIndex * 3 + 2;
		char q = find(qIndex, leftSet, rightSet, resultSet);
		char r = find(rIndex, leftSet, rightSet, resultSet);		
		char s = find(sIndex, leftSet, rightSet, resultSet);
		// We take into account the proposed assignment to the outer table set
		boolean tIsQ = (t % 3 == 0);
		boolean tIsR = (t % 3 == 1);
		boolean tIsS = (t % 3 == 2);
		if (tIsQ) {
			q = newPos;
		} else if (tIsR) {
			r = newPos;
		} else {
			assert(tIsS);
			s = newPos;
		}
		// Concatenate characters representing position of each table within triple
		String triplePositions = String.copyValueOf(new char[] {q, r, s});
		// Check whether this is one of the forbidden triples
		HashSet<String> forbidden = forbiddenTriples.get(constraintType);
		return !forbidden.contains(triplePositions);
	}
	/*
	static boolean okAsOuterOrInnerBushy(int t, BitSet resultSet, BitSet leftSet, 
			BitSet rightSet, int nrConstraints, boolean[] constraintVector, char newPos) {
		// Verify that table is assigned either as outer/left or inner/right operand
		assert(newPos == 'L' || newPos == 'R') : newPos;
		// Obtain index of table triple
		int tripleIndex = t/3;
		// Extract constraint referring to this specific triple
		ConstraintType constraintType = readConstraint(tripleIndex, nrConstraints, constraintVector);
		// If no constraint defined then all assignments are admissible
		if (constraintType == NO_CONSTRAINT) {
			return true;
		}
		// We call the first table within each triple q, the second r, and the third s
		int qIndex = tripleIndex * 3;
		int rIndex = tripleIndex * 3 + 1;
		int sIndex = tripleIndex * 3 + 2;
		// Switch depending on whether t is q, r, or s
		switch (t % 3) {
		case 0:
			// t is q
			if (constraintType == Q_PRECEDES_R) {
				
			}
			break;
		case 1:
			// t is r
			break;
		case 2:
			// t is s
			break;
		default:
			assert(false);
		}
		
		
		
		char q = find(qIndex, leftSet, rightSet, resultSet);
		char r = find(rIndex, leftSet, rightSet, resultSet);		
		char s = find(sIndex, leftSet, rightSet, resultSet);
		// We take into account the proposed assignment to the outer table set
		boolean tIsQ = (t % 3 == 0);
		boolean tIsR = (t % 3 == 1);
		boolean tIsS = (t % 3 == 2);
		if (tIsQ) {
			q = 'L';
		} else if (tIsR) {
			r = 'L';
		} else {
			assert(tIsS);
			s = 'L';
		}
		// Concatenate characters representing position of each table within triple
		String triplePositions = String.copyValueOf(new char[] {q, r, s});
		System.out.println(triplePositions);
		// Check whether this is one of the forbidden triples
		HashSet<String> forbidden = forbiddenTriples.get(constraintType);
		return !forbidden.contains(triplePositions);
	}
	*/
	/**
	 * Tries different splits of a given table set into two join operands and prunes with corresponding
	 * query plans. This version of the function assumes that the outer and inner operand are already
	 * partially specified and extends them by one more table such that all constraints are respected.
	 * If no tables remain unassigned then corresponding plans are generated and pruned.
	 * 
	 * @param query				query being optimized
	 * @param relations			maps table sets to corresponding relations
	 * @param resultTables		we consider alternative plans for joining that table set
	 * @param nrConstraints		the number of constraints describing the current search space partition
	 * @param constraintVector	a Boolean vector representing the constraint limiting search partition
	 * @param localAlpha		approximation factor used during pruning
	 * @param consideredMetrics	Boolean flags indicating which plan cost metrics are considered
	 * @param leftSet			tables that are already assigned to outer join operand
	 * @param rightSet			tables that are already assigned to inner join operand
	 * @param unassignedTables	tables that are currently unassigned
	 */
	static void trySplitsBushyRec(Query query, Map<BitSet, Relation> relations, BitSet resultTables, 
			int nrConstraints, boolean[] constraintVector, PlanSpace planSpace, CostModel costModel, 
			double localAlpha, boolean[] consideredMetrics, BitSet leftSet, BitSet rightSet, 
			BitSet unassignedTables) {
		// If all tables are assigned then we try all plans for the given split
		if (unassignedTables.isEmpty()) {
			if (!leftSet.isEmpty() && !rightSet.isEmpty()) {
				tryPlansOperators(query, relations, leftSet, rightSet, resultTables, 
						planSpace, costModel, localAlpha, consideredMetrics);				
			}
		} else {
			// Iterate over tables that are not yet assigned to either outer or inner join input
			for (int q = unassignedTables.nextSetBit(0); q >= 0; q = unassignedTables.nextSetBit(q+1)) {
				// Generate new set representing unassigned tables by taking out q
				BitSet newUnassigned = new BitSet();
				newUnassigned.or(unassignedTables);
				newUnassigned.clear(q);
				// Can we use current table in outer operand set?
				if (okAsOuterOrInnerBushy(q, resultTables, leftSet, 
						rightSet, nrConstraints, constraintVector, 'L')) {
					// Generate new outer operand
					BitSet newLeftSet = new BitSet();
					newLeftSet.or(leftSet);
					newLeftSet.set(q);
					// Recursive call
					trySplitsBushyRec(query, relations, resultTables, nrConstraints, constraintVector, 
							planSpace, costModel, localAlpha, consideredMetrics, 
							newLeftSet, rightSet, newUnassigned);
				}
				// Can we use current table in inner operand set?
				if (okAsOuterOrInnerBushy(q, resultTables, leftSet, 
						rightSet, nrConstraints, constraintVector, 'R')) {
					// Generate new inner operand
					BitSet newRightSet = new BitSet();
					newRightSet.or(rightSet);
					newRightSet.set(q);
					// Recursive call
					trySplitsBushyRec(query, relations, resultTables, nrConstraints, constraintVector,
							planSpace, costModel, localAlpha, consideredMetrics, 
							leftSet, newRightSet, newUnassigned);
				}
			}			
		} // whether still tables to assign
	}
	/**
	 * Consider all alternative plans for generating the specified result tables, trying out
	 * alternative splits into two join operand table sets within a bushy plan search space.
	 * 
	 * @param query					query for which to find optimal plans
	 * @param relations				maps table sets to corresponding relations
	 * @param resultTables			we consider alternative plans for joining that table set
	 * @param partitionID			the ID of the current search space partition
	 * @param nrPartitions			the total number of search space partitions
	 * @param planSpace				determines the set of applicable scan and join operators
	 * @param costModel				used to estimate the execution costs of query plans for multiple metrics
	 * @param localAlpha			approximation factor used during pruning
	 * @param consideredMetrics		Boolean flags indicating which plan cost metrics are considered
	 */
	/**
	 * Try all splits of a result set into two subsets representing join operands.
	 * This version of the function is specific to the linear join order space and considers
	 * only single tables as outer operand.
	 * 
	 * @param query					the query to optimize
	 * @param relations				maps table sets to the corresponding relations
	 * @param resultTables			the join result table set to split into two subsets 
	 * @param partitionID			ID of plan space partition being optimized within
	 * @param nrPartitions			the total number of join order space partitions
	 * @param planSpace				determines the set of applicable scan and join operators
	 * @param costModel				used to estimate the execution cost of query plans for multiple metrics
	 * @param localAlpha			approximation factor used during pruning
	 * @param consideredMetrics		Boolean flags indicating which plan cost metrics are considered
	 */
	static void trySplitsBushy(Query query, Map<BitSet, Relation> relations, BitSet resultTables, 
			int partitionID, int nrPartitions, PlanSpace planSpace, CostModel costModel,  
			double localAlpha, boolean[] consideredMetrics) {
		// Get dimension values
		int nrTables = query.nrTables;
		int nrTriples = nrTables / 3;
		// Get constraints on triples of tables
		int nrConstraints = (int)MathUtil.logOfBase(2, nrPartitions);
		boolean[] constraintVector = MathUtil.toBitVector(partitionID, nrConstraints);
		ConstraintType[] tripleConstraints = new ConstraintType[nrConstraints];
		for (int tripleIndex=0; tripleIndex<nrConstraints; ++tripleIndex) {
			tripleConstraints[tripleIndex] = readConstraint(tripleIndex, nrConstraints, constraintVector);
		}
		// Generate admissible operand subsets for each table triple
		ArrayList<List<BitSet>> admissibleTripleSubsets = new ArrayList<List<BitSet>>();
		for (int tripleIndex=0; tripleIndex<nrTriples; ++tripleIndex) {
			// Calculate indices of concerned tables
			int qIndex = tripleIndex * 3;
			int rIndex = tripleIndex * 3 + 1;
			int sIndex = tripleIndex * 3 + 2;
			// Determine set of concerned tables
			BitSet tripleTables = new BitSet();
			tripleTables.set(qIndex);
			tripleTables.set(rIndex);
			tripleTables.set(sIndex);
			// Determine set of contained tables
			BitSet containedTripleTables = new BitSet();
			containedTripleTables.or(tripleTables);
			containedTripleTables.and(resultTables);
			int nrContained = containedTripleTables.cardinality();
			// Generate list of admissible subsets
			List<BitSet> curTripleSubsets = new LinkedList<BitSet>();
			// If no constraints defined or not all tables present then any subset is admissible
			if (tripleIndex >= nrConstraints ||
					tripleConstraints[tripleIndex] == NO_CONSTRAINT ||
					!containedTripleTables.get(qIndex) ||
					!containedTripleTables.get(rIndex) ||
					!containedTripleTables.get(sIndex)) {
				// Iterate over cardinality of contained tables subsets
				for (int kContained=0; kContained<=nrContained; ++kContained) {
					// Iterate over subsets of the contained tables in this triple
					BitSetIterator containedIter = new BitSetIterator(containedTripleTables, kContained);
					while (containedIter.hasNext()) {
						BitSet subset = containedIter.next();
						curTripleSubsets.add(subset);
					}
				}
			} else {
				// Otherwise only some of the subsets are admissible ...
				// Translate constraint into successor and predecessor tables
				ConstraintType constraint = tripleConstraints[tripleIndex];
				assert(constraint != NO_CONSTRAINT);
				int predecessor = constraint == Q_PRECEDES_R ? qIndex : rIndex;
				int successor = constraint == Q_PRECEDES_R ? rIndex : qIndex;
				{
					BitSet subset = new BitSet();
					subset.set(qIndex);
					subset.set(rIndex);
					subset.set(sIndex);
					curTripleSubsets.add(subset);
				}
				{
					BitSet subset = new BitSet();
					curTripleSubsets.add(subset);
				}
				{
					BitSet subset = new BitSet();
					subset.set(qIndex);
					subset.set(rIndex);
					curTripleSubsets.add(subset);
				}
				{
					BitSet subset = new BitSet();
					subset.set(sIndex);
					curTripleSubsets.add(subset);
				}
				{
					BitSet subset = new BitSet();
					subset.set(predecessor);
					subset.set(sIndex);
					curTripleSubsets.add(subset);
				}
				{
					BitSet subset = new BitSet();
					subset.set(successor);
					curTripleSubsets.add(subset);
				}
			}
			admissibleTripleSubsets.add(tripleIndex, curTripleSubsets);
		}
		// Determine number of admissible split combinations (not taking into account commutativity)
		int nrSplits = 1;
		for (int tripleCtr=0; tripleCtr<nrTriples; ++tripleCtr) {
			List<BitSet> curTripleSubsets = admissibleTripleSubsets.get(tripleCtr);
			int nrSubsets = curTripleSubsets.size();
			nrSplits *= nrSubsets;
		}
		// Enumerate all possible splits based on admissible subsets
		for (int splitCtr=0; splitCtr<nrSplits; ++splitCtr) {
			// Generate admissible operand
			int remainder = splitCtr;
			BitSet operand = new BitSet();
			for (int tripleCtr=0; tripleCtr<nrTriples; ++tripleCtr) {
				List<BitSet> curTripleSubsets = admissibleTripleSubsets.get(tripleCtr);
				int nrSubsets = curTripleSubsets.size();
				int subsetIndex = remainder % nrSubsets;
				BitSet curSubset = curTripleSubsets.get(subsetIndex);
				operand.or(curSubset);
				remainder /= nrSubsets;
			}
			BitSet otherOperand = new BitSet();
			otherOperand.or(resultTables);
			otherOperand.andNot(operand);
			// Try corresponding join orders if operand is non-empty
			if (!operand.isEmpty() && !otherOperand.isEmpty()) {
				tryPlansOperators(query, relations, operand, otherOperand, 
						resultTables, planSpace, costModel, localAlpha, 
						consideredMetrics);	
			}
		}
	}

	/*
	static void trySplitsBushy(Query query, Map<BitSet, Relation> relations, BitSet resultTables, 
			int partitionID, int nrPartitions, PlanSpace planSpace, CostModel costModel,  
			double localAlpha, boolean[] consideredMetrics) {
		// Get dimension values
		int nrTables = query.nrTables;
		int nrTriples = nrTables / 3;
		// Get constraints on triples of tables
		int nrConstraints = (int)MathUtil.logOfBase(2, nrPartitions);
		boolean[] constraintVector = MathUtil.toBitVector(partitionID, nrConstraints);
		ConstraintType[] tripleConstraints = new ConstraintType[nrConstraints];
		for (int tripleIndex=0; tripleIndex<nrConstraints; ++tripleIndex) {
			tripleConstraints[tripleIndex] = readConstraint(tripleIndex, nrConstraints, constraintVector);
		}
		// Collect unconstrained tables and constrained triples
		BitSet unconstrainedTables = new BitSet();
		List<Integer> constrainedTriples = new LinkedList<Integer>();
		for (int tripleIndex=0; tripleIndex<nrTriples; ++tripleIndex) {
			// Calculate indices of concerned tables
			int qIndex = tripleIndex * 3;
			int rIndex = tripleIndex * 3 + 1;
			int sIndex = tripleIndex * 3 + 2;
			// Determine set of concerned tables
			BitSet tripleTables = new BitSet();
			tripleTables.set(qIndex);
			tripleTables.set(rIndex);
			tripleTables.set(sIndex);
			// Determine set of contained tables
			BitSet containedTripleTables = new BitSet();
			containedTripleTables.or(tripleTables);
			containedTripleTables.and(resultTables);
			// Determine whether we can consider all triple tables as unconstrained
			if (tripleIndex >= nrConstraints || tripleConstraints[tripleIndex] == NO_CONSTRAINT ||
					!containedTripleTables.get(qIndex) ||
					!containedTripleTables.get(rIndex) ||
					!containedTripleTables.get(sIndex)) {
				unconstrainedTables.or(containedTripleTables);
			} else {
				constrainedTriples.add(tripleIndex);
			}
		}
		int nrUnconstrainedTables = unconstrainedTables.cardinality();
		int nrConstrainedTriples = constrainedTriples.size();
		long nrCombinations = 1;
		for (int tripleCtr=0; tripleCtr<nrConstrainedTriples; ++tripleCtr) {
			nrCombinations *= 6;
		}
		// Iterate over all  splits for unconstrained tables
		for (int kUnconstrainedLeft=0; kUnconstrainedLeft<=nrUnconstrainedTables; ++kUnconstrainedLeft) {
			BitSetIterator unconstrainedLeftIter = new BitSetIterator(unconstrainedTables, kUnconstrainedLeft);
			// For all possible left operands with given cardinality
			while (unconstrainedLeftIter.hasNext()) {
				BitSet unconstrainedLeftTables = unconstrainedLeftIter.next();
				// Right operand is complement of left operand in result table set
				BitSet unconstrainedRightTables = (BitSet)unconstrainedTables.clone();
				unconstrainedRightTables.andNot(unconstrainedLeftTables);
				// Add tables belonging to constrained triples if there are any
				if (nrConstrainedTriples > 0) {
					for (int splitCtr=0; splitCtr<nrCombinations; ++splitCtr) {
						BitSet leftTables = new BitSet();
						BitSet rightTables = new BitSet();
						leftTables.or(unconstrainedLeftTables);
						rightTables.or(unconstrainedRightTables);
						int remainder = splitCtr;
						Iterator<Integer> tripleIndices = constrainedTriples.iterator();
						for (int tripleCtr=0; tripleCtr<nrConstrainedTriples; ++tripleCtr) {
							// Get index of triple split to use
							int tripleSplitIndex = remainder % 6;
							remainder /= 6;
							// Get index of triple to split
							int tripleIndex = tripleIndices.next();
							// Calculate indices of concerned tables
							int qIndex = tripleIndex * 3;
							int rIndex = tripleIndex * 3 + 1;
							int sIndex = tripleIndex * 3 + 2;
							// Translate constraint into successor and predecessor tables
							ConstraintType constraint = tripleConstraints[tripleIndex];
							assert(constraint != NO_CONSTRAINT);
							int predecessor = constraint == Q_PRECEDES_R ? qIndex : rIndex;
							int successor = constraint == Q_PRECEDES_R ? rIndex : qIndex;
							switch (tripleSplitIndex) {
							// Assign entire triple to one of the two operands
							case 0:
								leftTables.set(qIndex);
								leftTables.set(rIndex);
								leftTables.set(sIndex);
								break;
							case 1:
								rightTables.set(qIndex);
								rightTables.set(rIndex);
								rightTables.set(sIndex);
								break;
							// Assign q and r to one operand and s to the other one 
							case 2:
								leftTables.set(qIndex);
								leftTables.set(rIndex);
								rightTables.set(sIndex);							
								break;
							case 3:
								rightTables.set(qIndex);
								rightTables.set(rIndex);
								leftTables.set(sIndex);
								break;
							// Assign predecessor and s to one and the successor to the other one
							case 4:
								leftTables.set(predecessor);
								leftTables.set(sIndex);
								rightTables.set(successor);
								break;
							case 5:
								rightTables.set(predecessor);
								rightTables.set(sIndex);
								leftTables.set(successor);
								break;
							default:
								assert(false);
							}
							// Try join operators if none of the operands is empty
							if (!leftTables.isEmpty() && !rightTables.isEmpty()) {
								// Generate all plans that correspond to current result set split
								System.out.println("leftTables: " + leftTables.toString());
								System.out.println("rightTables: " + rightTables.toString());
								System.out.println("resultTables: " + resultTables.toString());
								System.out.println("unconstrainedTables: " + unconstrainedTables.toString());
								System.out.println("constrainedTriples: " + constrainedTriples.toString());
								System.out.println("successor: " + successor);
								System.out.println("predecessor: " + predecessor);
								for (int constraintCtr=0; constraintCtr<nrConstraints; ++constraintCtr) {
									System.out.println("constraint " + constraintCtr + ":" + tripleConstraints[constraintCtr]);
								}
								tryPlansOperators(query, relations, leftTables, rightTables, 
										resultTables, planSpace, costModel, localAlpha, 
										consideredMetrics);							
							}
						}
					} // splitCtr					
				} else {
					// no constrained triples to process
					if (!unconstrainedLeftTables.isEmpty() && !unconstrainedRightTables.isEmpty()) {
						// Generate all plans that correspond to current result set split
						tryPlansOperators(query, relations, unconstrainedLeftTables, 
								unconstrainedRightTables, resultTables, planSpace, 
								costModel, localAlpha, consideredMetrics);							
					}
				}
			}
		}
	}
	*/
	/*
	static void trySplitsBushy(Query query, Map<BitSet, Relation> relations, BitSet resultTables, 
			int partitionID, int nrPartitions, PlanSpace planSpace, CostModel costModel,  
			double localAlpha, boolean[] consideredMetrics) {
		// We consider all splits of the result tables into two subsets whose relations are included.
		int nrResultTables = resultTables.cardinality();
		// Iterate over cardinality of result subset that forms left operand for final join
		for (int kLeft=1; kLeft<nrResultTables; ++kLeft) {
			BitSetIterator leftIter = new BitSetIterator(resultTables, kLeft);
			// For all possible left operands with given cardinality
			while (leftIter.hasNext()) {
				BitSet leftTables = leftIter.next();
				// Right operand is complement of left operand in result table set
				BitSet rightTables = (BitSet)resultTables.clone();
				rightTables.andNot(leftTables); 
				// Continue only if both operands were included before
				if (relations.containsKey(leftTables) && relations.containsKey(rightTables)) {
					// Generate all plans that correspond to current result set split
					tryPlansOperators(query, relations, leftTables, rightTables, resultTables, 
							planSpace, costModel, localAlpha, consideredMetrics);
				}
			}
		}
	}
	*/
	/*
	static void trySplitsBushy(Query query, Map<BitSet, Relation> relations, BitSet resultTables, 
			int partitionID, int nrPartitions, PlanSpace planSpace, CostModel costModel, 
			double localAlpha, boolean[] consideredMetrics) {
		// Extract constraints
		int nrConstraints = (int)MathUtil.logOfBase(2, nrPartitions);
		boolean[] constraintVector = MathUtil.toBitVector(partitionID, nrConstraints);
		trySplitsBushyRec(query, relations, resultTables, nrConstraints, constraintVector, 
				planSpace, costModel, localAlpha, consideredMetrics, 
				new BitSet(), new BitSet(), resultTables);
	}
	*/
	/**
	 * Returns best query plans in current search space partition.
	 * 
	 * @param query					the query to optimize
	 * @param joinOrderSpace		whether linear or bushy query plans are considered
	 * @param planSpace				determines the set of applicable scan and join operators
	 * @param costModel				estimates the execution cost of query plans for multiple metrics 
	 * @param consideredMetrics		Boolean flags indicating which plan cost metrics are considered
	 * @param globalAlpha			target approximation factor
	 * @param partitionID			identifier of current search space partition
	 * @param nrPartitions			total number of search space partitions
	 * @return						a set of Pareto-optimal plans within current search space partition
	 * 								and the amount of main memory consumed during this invocation
	 */
	public static PartitioningSlaveResult optimize(Query query, JoinOrderSpace joinOrderSpace, 
			PlanSpace planSpace, CostModel costModel, boolean[] consideredMetrics, 
			double globalAlpha, int partitionID, int nrPartitions, long timeoutMillis) {
		//System.out.println("Started optimization by partitioning slave");
		//System.err.println("Started optimization by partitioning slave");
		try{
		// initialize timers
		long startMillis = System.currentTimeMillis();
		// initialize variables
		int nrTables = query.nrTables;
		assert(nrTables % 2 == 0 && joinOrderSpace.equals(JoinOrderSpace.LINEAR)) ||
			(nrTables % 3 == 0 && joinOrderSpace.equals(JoinOrderSpace.BUSHY)): 
				"Table number must be multiple of two (three) for linear (bushy) join order spaces!";
		// Calculate local alpha from global alpha
		double localAlpha = Math.pow(globalAlpha, 1.0/nrTables);
		// initialize index of full query table set
		BitSet allTablesSet = new BitSet();
		for (int tableIndex=0; tableIndex<nrTables; ++tableIndex) {
			allTablesSet.set(tableIndex);
		}
		// maps table sets to corresponding relations
		Map<BitSet, Relation> relations = new HashMap<BitSet, Relation>();
		// treat single table relations
		for (int tableIndex=0; tableIndex<nrTables; ++tableIndex) {
			// create relation
			Relation rel = RelationFactory.createSingleTableRel(query, tableIndex);
			// iterate over scan operators
			for (ScanOperator scanOp : planSpace.scanOperatorsShuffled(rel)) {
				Plan scanPlan = new ScanPlan(rel.cardinality, rel.pages, tableIndex, scanOp);
				costModel.updateRoot(scanPlan);
				PruningUtil.prune(query, rel, scanPlan, localAlpha, consideredMetrics, false);
			}
			// create relation index
			BitSet tableSet = new BitSet();
			tableSet.set(tableIndex);
			// insert relation
			relations.put(tableSet, rel);
		}
		// Obtain table result sets in current search space partition
		List<List<BitSet>> resultTableSets = generateResultTableSets(
				nrTables, partitionID, nrPartitions, joinOrderSpace);
		// treat larger table sets in ascending order of cardinality
		for (int k=2; k<=nrTables; ++k) {
			// for all table sets of cardinality k
			for (BitSet resultSet : resultTableSets.get(k-1)) {
				// Try different splits
				switch (joinOrderSpace) {
				case LINEAR:
					trySplitsLinear(query, relations, resultSet, partitionID, nrPartitions, 
							planSpace, costModel, localAlpha, consideredMetrics);
					break;
				case BUSHY:
					trySplitsBushy(query, relations, resultSet, partitionID, nrPartitions, 
							planSpace, costModel, localAlpha, consideredMetrics);
					break;
				default:
					assert(false);
				}
				// check for timeouts
				long elapsedMillis = System.currentTimeMillis() - startMillis;
				if (elapsedMillis > timeoutMillis) {
					int mainMemoryConsumed = relations.size();
					return new PartitioningSlaveResult(null, mainMemoryConsumed, 
							elapsedMillis, true, false, null);
				}
			} // over result table set
		} // over result table set cardinality
		// return Pareto plans for joining all tables
		Relation resultRel = relations.get(allTablesSet);
		List<Plan> resultPlans = resultRel.ParetoPlans;
		// We measure the amount of main memory by the number of generated relations
		int mainMemoryConsumed = relations.size();
		long elapsedMillis = System.currentTimeMillis() - startMillis;
		return new PartitioningSlaveResult(resultPlans, mainMemoryConsumed, 
				elapsedMillis, false, false, null);
		} catch (Throwable t) {
			System.out.println("*** THROWABLE IN SLAVE ***");
			System.err.println("*** THROWABLE IN SLAVE ***");
			t.printStackTrace();
			String stackTraceString = "";
			for (StackTraceElement trace : t.getStackTrace()) {
				stackTraceString += trace.toString() + ";";
			}
			return new PartitioningSlaveResult(null, Long.MAX_VALUE, 
					Long.MAX_VALUE, false, true, stackTraceString);
		}
	}
	/**
	 * The following version of the optimization function facilitates the invocation
	 * as a map operation over a Spark RDD.
	 * 
	 * @param slaveTask	encapsulates a description of the optimization task to perform by the slave
	 * @return						a set of Pareto-optimal plans within current search space partition
	 * 								and the amount of main memory consumed during this invocation
	 */
	public static PartitioningSlaveResult optimize(PartitioningSlaveTask slaveTask) {
		return optimize(slaveTask.query, slaveTask.joinOrderSpace, slaveTask.planSpace, 
				slaveTask.costModel, slaveTask.consideredMetrics, slaveTask.alpha, 
				slaveTask.partitionID, slaveTask.nrPartitions, slaveTask.timeoutMillis);
	}
}
