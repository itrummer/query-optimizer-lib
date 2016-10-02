package optimizer.parallelized.partitioning;

import static org.junit.Assert.*;
//import static util.TestUtil.*;
import static optimizer.parallelized.partitioning.ConstraintType.*;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import benchmark.Statistics;
import common.RandomNumbers;
import cost.MultiCostModel;
import cost.SingleCostModel;
import cost.local.TimeCostModel;
import optimizer.approximate.DPmoqo;
import plans.JoinOrderSpace;
import plans.ParetoPlanSet;
import plans.Plan;
import plans.spaces.LocalPlanSpace;
import plans.spaces.PlanSpace;
import queries.JoinGraphType;
import queries.JoinType;
import queries.Query;
import queries.QueryFactory;
import util.MathUtil;
import util.ParetoUtil;
import util.TestUtil;

import org.junit.Test;

public class PartitioningSlaveTest {

	@Test
	public void test() {
		// Reading constraints
		{
			int partitionID = 5;
			int nrConstraints = 4;
			boolean[] constraintVector = MathUtil.toBitVector(partitionID, nrConstraints);
			assertEquals(Q_PRECEDES_R, PartitioningSlave.readConstraint(0, nrConstraints, constraintVector));
			assertEquals(R_PRECEDES_Q, PartitioningSlave.readConstraint(1, nrConstraints, constraintVector));
			assertEquals(Q_PRECEDES_R, PartitioningSlave.readConstraint(2, nrConstraints, constraintVector));
			assertEquals(R_PRECEDES_Q, PartitioningSlave.readConstraint(3, nrConstraints, constraintVector));
			assertEquals(NO_CONSTRAINT, PartitioningSlave.readConstraint(4, nrConstraints, constraintVector));
			assertEquals(NO_CONSTRAINT, PartitioningSlave.readConstraint(5, nrConstraints, constraintVector));
			assertEquals(NO_CONSTRAINT, PartitioningSlave.readConstraint(6, nrConstraints, constraintVector));
		}
		{
			int partitionID = 15;
			int nrConstraints = 4;
			boolean[] constraintVector = MathUtil.toBitVector(partitionID, nrConstraints);
			assertEquals(Q_PRECEDES_R, PartitioningSlave.readConstraint(0, nrConstraints, constraintVector));
			assertEquals(Q_PRECEDES_R, PartitioningSlave.readConstraint(1, nrConstraints, constraintVector));
			assertEquals(Q_PRECEDES_R, PartitioningSlave.readConstraint(2, nrConstraints, constraintVector));
			assertEquals(Q_PRECEDES_R, PartitioningSlave.readConstraint(3, nrConstraints, constraintVector));
			assertEquals(NO_CONSTRAINT, PartitioningSlave.readConstraint(4, nrConstraints, constraintVector));
			assertEquals(NO_CONSTRAINT, PartitioningSlave.readConstraint(5, nrConstraints, constraintVector));
			assertEquals(NO_CONSTRAINT, PartitioningSlave.readConstraint(6, nrConstraints, constraintVector));
		}
		// Generating table sets for linear plan space partitions
		{
			int nrTables = 2;
			int nrConstraints = 1;
			boolean[] constraintVector = new boolean[] {true};
			// Constraint vector specifies that first table needs to be joined before second.
			// Table sets that contain the second table but not the first are therefore not
			// admissible as intermediate results and won't be generated.
			Set<BitSet> expectedSets = new HashSet<BitSet>();
			// Attention: lower weights are at the right of the bit strings
			expectedSets.addAll(Arrays.asList(new BitSet[] {
					MathUtil.getBitSet("00"),
					MathUtil.getBitSet("01"),
					MathUtil.getBitSet("11"),
			}));
			// Generate bit sets
			List<BitSet> tableSetsList = PartitioningSlave.constraintTableSetsLinear(
					nrTables, nrConstraints, constraintVector);
			// Verify cardinality
			assertEquals(3, tableSetsList.size());
			// Verify content
			Set<BitSet> tableSetsSet = new HashSet<BitSet>();
			tableSetsSet.addAll(tableSetsList);
			assertEquals(expectedSets, tableSetsSet);
		}
		{
			int nrTables = 2;
			int nrConstraints = 1;
			boolean[] constraintVector = new boolean[] {false};
			// Constraint vector specifies that second table needs to be joined before first.
			// Table sets that contain the first table but not the second are therefore not
			// admissible as intermediate results and won't be generated.
			Set<BitSet> expectedSets = new HashSet<BitSet>();
			// Attention: lower weights are at the right of the bit strings
			expectedSets.addAll(Arrays.asList(new BitSet[] {
					MathUtil.getBitSet("00"),
					MathUtil.getBitSet("10"),
					MathUtil.getBitSet("11"),
			}));
			// Generate bit sets
			List<BitSet> tableSetsList = PartitioningSlave.constraintTableSetsLinear(
					nrTables, nrConstraints, constraintVector);
			// Verify cardinality
			assertEquals(3, tableSetsList.size());
			// Verify content
			Set<BitSet> tableSetsSet = new HashSet<BitSet>();
			tableSetsSet.addAll(tableSetsList);
			assertEquals(expectedSets, tableSetsSet);
		}
		{
			int nrTables = 4;
			int nrConstraints = 2;
			boolean[] constraintVector = new boolean[] {true, false};
			// Constraint vector specifies that first table needs to be joined before second and
			// that fourth table needs to be joined before the third.
			// Table sets that contain the second table but not the first or the third table but
			// not the fourth are therefore not admissible as intermediate results.
			Set<BitSet> expectedSets = new HashSet<BitSet>();
			// Attention: lower weights are at the right of the bit strings
			expectedSets.addAll(Arrays.asList(new BitSet[] {
					MathUtil.getBitSet("00"),
					MathUtil.getBitSet("01"),
					MathUtil.getBitSet("11"),
					MathUtil.getBitSet("1000"),
					MathUtil.getBitSet("1001"),
					MathUtil.getBitSet("1011"),
					MathUtil.getBitSet("1100"),
					MathUtil.getBitSet("1101"),
					MathUtil.getBitSet("1111"),
			}));
			// Generate bit sets
			List<BitSet> tableSetsList = PartitioningSlave.constraintTableSetsLinear(
					nrTables, nrConstraints, constraintVector);
			// Verify cardinality
			assertEquals(9, tableSetsList.size());
			// Verify content
			Set<BitSet> tableSetsSet = new HashSet<BitSet>();
			tableSetsSet.addAll(tableSetsList);
			assertEquals(expectedSets, tableSetsSet);
		}
		// Testing generation of constrained table sets in bushy plan space
		{
			int nrTables = 3;
			int nrConstraints = 1;
			boolean[] constraintVector = new boolean[] {true};
			// Constraint vector specifies that the first table needs to be joined before the second
			// if the third table is present. This means that table sets where the second table and the
			// third table are present but not the first one are inadmissible.
			Set<BitSet> expectedSets = MathUtil.powerSet(2);
			expectedSets.remove(MathUtil.getBitSet("110"));
			// Generate bit sets
			List<BitSet> tableSetsList = PartitioningSlave.constraintTableSetsBushy(
					nrTables, nrConstraints, constraintVector);
			// Verify cardinality
			assertEquals(7, tableSetsList.size());
			// Verify content
			Set<BitSet> tableSetsSet = new HashSet<BitSet>();
			tableSetsSet.addAll(tableSetsList);
			assertEquals(expectedSets, tableSetsSet);
		}
		{
			int nrTables = 6;
			int nrConstraints = 2;
			boolean[] constraintVector = new boolean[] {true, false};
			// Constraint vector: q_1 <= q_2 | q_3; q_5 <= q_4 | q_6
			Set<BitSet> expectedSets = MathUtil.powerSet(5);
			// Those sets violate the first constraint
			expectedSets.remove(MathUtil.getBitSet("000110"));
			expectedSets.remove(MathUtil.getBitSet("001110"));
			expectedSets.remove(MathUtil.getBitSet("010110"));
			expectedSets.remove(MathUtil.getBitSet("011110"));
			expectedSets.remove(MathUtil.getBitSet("100110"));
			expectedSets.remove(MathUtil.getBitSet("101110"));
			expectedSets.remove(MathUtil.getBitSet("110110"));
			expectedSets.remove(MathUtil.getBitSet("111110"));
			// Those sets violate the second constraint
			expectedSets.remove(MathUtil.getBitSet("101000"));
			expectedSets.remove(MathUtil.getBitSet("101001"));
			expectedSets.remove(MathUtil.getBitSet("101010"));
			expectedSets.remove(MathUtil.getBitSet("101011"));
			expectedSets.remove(MathUtil.getBitSet("101100"));
			expectedSets.remove(MathUtil.getBitSet("101101"));
			expectedSets.remove(MathUtil.getBitSet("101110"));
			expectedSets.remove(MathUtil.getBitSet("101111"));
			// Generate bit sets
			List<BitSet> tableSetsList = PartitioningSlave.constraintTableSetsBushy(
					nrTables, nrConstraints, constraintVector);
			// Verify cardinality
			assertEquals(49, tableSetsList.size());
			// Verify content
			Set<BitSet> tableSetsSet = new HashSet<BitSet>();
			tableSetsSet.addAll(tableSetsList);
			assertEquals(expectedSets, tableSetsSet);
		}
		// Generate result table sets and order them by cardinality
		{
			int nrTables = 2;
			// Constraint vector specifies that second table needs to be joined before first.
			// Table sets that contain the first table but not the second are therefore not
			// admissible as intermediate results and won't be generated.
			Set<BitSet> expectedSets = new HashSet<BitSet>();
			// Attention: lower weights are at the right of the bit strings
			expectedSets.addAll(Arrays.asList(new BitSet[] {
					MathUtil.getBitSet("00"),
					MathUtil.getBitSet("10"),
					MathUtil.getBitSet("11"),
			}));
			// Generate bit sets
			List<List<BitSet>> tableSetsBySize = 
					PartitioningSlave.generateResultTableSets(nrTables, 0, 2, JoinOrderSpace.LINEAR);
			// Verify cardinalities
			assertEquals(1, tableSetsBySize.get(0).size());
			assertEquals(1, tableSetsBySize.get(1).size());
			// Verify content
			assertEquals(MathUtil.getBitSet("10"), tableSetsBySize.get(0).get(0));
			assertEquals(MathUtil.getBitSet("11"), tableSetsBySize.get(1).get(0));
		}
		{
			int nrTables = 4;
			// Generate bit sets
			List<List<BitSet>> tableSetsBySize = 
					PartitioningSlave.generateResultTableSets(nrTables, 2, 4, JoinOrderSpace.LINEAR);
			// Verify cardinalities
			assertEquals(2, tableSetsBySize.get(0).size());
			assertEquals(3, tableSetsBySize.get(1).size());
			assertEquals(2, tableSetsBySize.get(2).size());
			assertEquals(1, tableSetsBySize.get(3).size());
		}
		{
			int nrTables = 3;
			int partitionID = 0;
			int nrPartitions = 2;
			// Generate bit sets
			List<List<BitSet>> tableSetsBySize = PartitioningSlave.generateResultTableSets(
					nrTables, partitionID, nrPartitions, JoinOrderSpace.BUSHY);
			// Verify cardinalities
			assertEquals(3, tableSetsBySize.get(0).size());
			assertEquals(2, tableSetsBySize.get(1).size());
			assertEquals(1, tableSetsBySize.get(2).size());
		}
		// Test generating plans for given split
		// TODO
		// Check whether suitable inner operands in linear plan space partitions are correctly identified
		{
			BitSet resultSet = MathUtil.getBitSet("1111");
			// First table precedes second table
			boolean[] constraintVector = new boolean[] {true};
			int nrConstraints = constraintVector.length;
			// As the result set includes the second table, the first table cannot be taken out
			assertEquals(false, PartitioningSlave.okAsInnerLinear(0, resultSet, nrConstraints, constraintVector));
			assertEquals(true, PartitioningSlave.okAsInnerLinear(1, resultSet, nrConstraints, constraintVector));
		}
		{
			BitSet resultSet = MathUtil.getBitSet("1101");
			// First table precedes second table
			boolean[] constraintVector = new boolean[] {true};
			int nrConstraints = constraintVector.length;
			// As the result set does not include the second table, the first table can be taken out
			assertEquals(true, PartitioningSlave.okAsInnerLinear(0, resultSet, nrConstraints, constraintVector));
		}
		{
			BitSet resultSet = MathUtil.getBitSet("1110");
			// First table precedes second table; fourth table precedes third table
			boolean[] constraintVector = new boolean[] {true, false};
			int nrConstraints = constraintVector.length;
			// As the result set contains the fourth table, the fourth table cannot be taken out
			assertEquals(false, PartitioningSlave.okAsInnerLinear(3, resultSet, nrConstraints, constraintVector));
			assertEquals(true, PartitioningSlave.okAsInnerLinear(2, resultSet, nrConstraints, constraintVector));
		}
		// Test generating plans for all admissible splits in linear plan space partition
		// TODO
		// Test transforming the position of a table into a character
		{
			BitSet leftSet = MathUtil.getBitSet("1100");
			BitSet rightSet = MathUtil.getBitSet("0011");
			BitSet resultSet = MathUtil.getBitSet("1111");
			assertEquals('R', PartitioningSlave.find(0, leftSet, rightSet, resultSet));
			assertEquals('R', PartitioningSlave.find(1, leftSet, rightSet, resultSet));
			assertEquals('L', PartitioningSlave.find(2, leftSet, rightSet, resultSet));
			assertEquals('L', PartitioningSlave.find(3, leftSet, rightSet, resultSet));
		}
		{
			BitSet leftSet = MathUtil.getBitSet("1000");
			BitSet rightSet = MathUtil.getBitSet("0010");
			BitSet resultSet = MathUtil.getBitSet("1110");
			assertEquals('-', PartitioningSlave.find(0, leftSet, rightSet, resultSet));
			assertEquals('R', PartitioningSlave.find(1, leftSet, rightSet, resultSet));
			assertEquals('U', PartitioningSlave.find(2, leftSet, rightSet, resultSet));
			assertEquals('L', PartitioningSlave.find(3, leftSet, rightSet, resultSet));
		}
		// Test verifying whether specific tables are admissible in outer or inner join operands
		// in a bushy plan space partition.
		{
			BitSet leftSet = MathUtil.getBitSet("000000");
			BitSet rightSet = MathUtil.getBitSet("111110");
			BitSet resultSet = MathUtil.getBitSet("111111");
			{
				// Test: preceding table can only be assigned to one operand if activation table present
				
				// First table precedes second if third table is present;
				// Fourth table precedes fifth if sixth table is present
				boolean[] constraintVector = new boolean[] {true, true};
				int nrConstraints = constraintVector.length;
				// Cannot assign first table as left operand since third and second tables
				// would be present in right operand.
				assertEquals(false, PartitioningSlave.okAsOuterOrInnerBushy(0, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'L'));
				// Can assign in right operand (then the left join operand would be empty
				// but this is not checked in the tested function).
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(0, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'R'));				
			}
			{
				// Test: can do anything if no constraints defined
				
				boolean[] constraintVector = new boolean[] {};
				int nrConstraints = constraintVector.length;
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(0, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'L'));
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(0, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'R'));	
			}
			{
				// Test: can assign successor table arbitrarily if activation table present
				
				// Second table precedes first if third table is present;
				// Fifth table precedes fourth if sixth table is present
				boolean[] constraintVector = new boolean[] {false, false};
				int nrConstraints = constraintVector.length;
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(0, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'L'));
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(0, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'R'));				
			}
		}
		{
			BitSet leftSet = MathUtil.getBitSet("000000");
			BitSet rightSet = MathUtil.getBitSet("111010");
			BitSet resultSet = MathUtil.getBitSet("111011");
			{
				// Test: preceding table can be assigned arbitrarily if activation table not present
				
				// First table precedes second if third table is present;
				// Fourth table precedes fifth if sixth table is present
				boolean[] constraintVector = new boolean[] {true, true};
				int nrConstraints = constraintVector.length;
				// Cannot assign first table as left operand since third and second tables
				// would be present in right operand.
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(0, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'L'));
				// Can assign in right operand (then the left join operand would be empty
				// but this is not checked in the tested function).
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(0, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'R'));
			}
			{
				// Test: can do anything if no constraints defined
				
				boolean[] constraintVector = new boolean[] {};
				int nrConstraints = constraintVector.length;
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(0, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'L'));
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(0, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'R'));
			}
			{
				// Test: can assign successor table arbitrarily
				
				// Second table precedes first if third table is present;
				// Fifth table precedes fourth if sixth table is present
				boolean[] constraintVector = new boolean[] {false, false};
				int nrConstraints = constraintVector.length;
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(0, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'L'));
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(0, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'R'));
			}
		}
		{
			BitSet leftSet = MathUtil.getBitSet("000000");
			BitSet rightSet = MathUtil.getBitSet("111101");
			BitSet resultSet = MathUtil.getBitSet("111111");
			{
				// Test: successor table can be assigned arbitrarily even if activation table present
				
				// First table precedes second if third table is present;
				// Fourth table precedes fifth if sixth table is present
				boolean[] constraintVector = new boolean[] {true, true};
				int nrConstraints = constraintVector.length;
				// Can assign second table either as right or left operand
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(1, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'L'));
				// Can assign in right operand (then the left join operand would be empty
				// but this is not checked in the tested function).
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(1, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'R'));
			}
			{
				// Test: can do anything if no constraints defined
				
				boolean[] constraintVector = new boolean[] {};
				int nrConstraints = constraintVector.length;
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(1, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'L'));
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(1, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'R'));
			}
			{
				// Test: can only assign preceding table to right set if activation table present
				
				// Second table precedes first if third table is present;
				// Fifth table precedes fourth if sixth table is present
				boolean[] constraintVector = new boolean[] {false, false};
				int nrConstraints = constraintVector.length;
				assertEquals(false, PartitioningSlave.okAsOuterOrInnerBushy(1, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'L'));
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(1, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'R'));
			}
		}
		{
			BitSet leftSet = MathUtil.getBitSet("000000");
			BitSet rightSet = MathUtil.getBitSet("111001");
			BitSet resultSet = MathUtil.getBitSet("111011");
			{
				// Test: successor table can be assigned arbitrarily also if activation table is not present
				
				// First table precedes second if third table is present;
				// Fourth table precedes fifth if sixth table is present
				boolean[] constraintVector = new boolean[] {true, true};
				int nrConstraints = constraintVector.length;
				// Can assign second table either as right or left operand
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(1, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'L'));
				// Can assign in right operand (then the left join operand would be empty
				// but this is not checked in the tested function).
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(1, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'R'));
			}
			{
				// Test: can do anything if no constraints defined
				
				boolean[] constraintVector = new boolean[] {};
				int nrConstraints = constraintVector.length;
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(1, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'L'));
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(1, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'R'));
			}
			{
				// Test: preceding table can be assigned arbitrarily if activation table not present
				
				// Second table precedes first if third table is present;
				// Fifth table precedes fourth if sixth table is present
				boolean[] constraintVector = new boolean[] {false, false};
				int nrConstraints = constraintVector.length;
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(1, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'L'));
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(1, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'R'));
			}
		}
		{
			BitSet leftSet = MathUtil.getBitSet("000000");
			BitSet rightSet = MathUtil.getBitSet("111011");
			BitSet resultSet = MathUtil.getBitSet("111111");
			{
				// Test: activation table can be assigned arbitrarily
				
				// First table precedes second if third table is present;
				// Fourth table precedes fifth if sixth table is present
				boolean[] constraintVector = new boolean[] {true, true};
				int nrConstraints = constraintVector.length;
				// Can assign second table either as right or left operand
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(2, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'L'));
				// Can assign in right operand (then the left join operand would be empty
				// but this is not checked in the tested function).
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(2, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'R'));
			}
			{
				// Test: can do anything if no constraints defined
				
				boolean[] constraintVector = new boolean[] {};
				int nrConstraints = constraintVector.length;
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(2, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'L'));
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(2, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'R'));
			}
			{
				// Test: activation table can be assigned arbitrarily
				
				// Second table precedes first if third table is present;
				// Fifth table precedes fourth if sixth table is present
				boolean[] constraintVector = new boolean[] {false, false};
				int nrConstraints = constraintVector.length;
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(2, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'L'));
				assertEquals(true, PartitioningSlave.okAsOuterOrInnerBushy(2, resultSet, leftSet, 
						rightSet, nrConstraints, constraintVector, 'R'));
			}
		}
		// Test optimization: try whether results are consistent with non-parallel optimizer
		{
			PlanSpace planSpace = new LocalPlanSpace();
			MultiCostModel costModel = new MultiCostModel(Arrays.asList(new SingleCostModel[] {
					new TimeCostModel(0)
			}));
			Statistics.init(1, 1, 1, 1);
			Statistics.disable();
			for (JoinOrderSpace joinOrderSpace : JoinOrderSpace.values()) {
				System.out.println("Comparing single node and parallel optimizer in " + joinOrderSpace);
				for (int queryCtr=0; queryCtr<50; ++queryCtr) {
					// Generate query
					int nrJoinGraphTypes = JoinGraphType.values().length;
					int randomJoinGraphIndex = RandomNumbers.random.nextInt(nrJoinGraphTypes);
					JoinGraphType joinGraphType = JoinGraphType.values()[randomJoinGraphIndex];
					Query query = QueryFactory.produce(joinGraphType, 6, 1, JoinType.MN);
					// Consider only execution time to compare query plans
					//boolean[] consideredMetrics = new boolean[] {true};
					boolean[] consideredMetrics = new boolean[] {true, true};
					// Generate single-node and optimize query
					DPmoqo singleNodeDP = new DPmoqo(1, joinOrderSpace);
					ParetoPlanSet singleNodeParetoPlanSet = singleNodeDP.approximateParetoSet(
							query, consideredMetrics, planSpace, costModel, null, 0, 0, 0);
					// Solve same query with parallelized optimizer
					List<Plan> parallelParetoPlans = new LinkedList<Plan>();
					int nrPartitions = 4;
					for (int partitionID=0; partitionID<nrPartitions; ++partitionID) {
						PartitioningSlaveResult result = PartitioningSlave.optimize(query, 
								joinOrderSpace, planSpace, costModel, consideredMetrics, 
								1, partitionID, nrPartitions, Long.MAX_VALUE);
						parallelParetoPlans.addAll(result.paretoPlans);
					}
					// Plans from single-node and from parallel optimizer must cover the same cost values
					double epsilon1 = ParetoUtil.epsilonError(parallelParetoPlans, 
							singleNodeParetoPlanSet.plans, consideredMetrics);
					double epsilon2 = ParetoUtil.epsilonError(singleNodeParetoPlanSet.plans, 
							parallelParetoPlans, consideredMetrics);
					assertEquals(0, epsilon1, TestUtil.EPSILON);
					assertEquals(0, epsilon2, TestUtil.EPSILON);
				}
				System.out.println("Single node and parallel optimizer consistent for " + joinOrderSpace);;
			}

		}
	}

}
