package util;

import static org.junit.Assert.*;
import static util.TestUtil.*;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import common.Constants;
import cost.MultiCostModel;
import cost.SingleCostModel;
import cost.local.BufferCostModel;
import cost.local.DiscCostModel;
import cost.local.TimeCostModel;
import plans.JoinPlan;
import plans.PathNode;
import plans.Plan;
import plans.ScanPlan;
import plans.operators.JoinOperator;
import plans.operators.ScanOperator;
import plans.operators.local.BNLjoin;
import plans.operators.local.HashJoin;
import plans.operators.local.LocalScan;
import plans.operators.local.SortMergeJoin;
import plans.spaces.LocalPlanSpace;
import plans.spaces.PlanSpace;
import queries.JoinGraphType;
import queries.JoinType;
import queries.Query;
import queries.QueryFactory;
import relations.Relation;
import relations.RelationFactory;

import org.junit.Test;

public class LocalSearchUtilTest {

	@Test
	public void test() {
		Query query = QueryFactory.produce(JoinGraphType.CHAIN, 10, 100000, JoinType.MN);
		PlanSpace planSpace = new LocalPlanSpace();
		List<SingleCostModel> costModels = Arrays.asList(new SingleCostModel[] {
				new TimeCostModel(0), new BufferCostModel(1), new DiscCostModel(2)
		});
		MultiCostModel costModel = new MultiCostModel(costModels);
		//Plan plan = LocalSearchUtil.randomBushyPlan(query, planSpace, tableIndices);
		// Test popping relations
		{
			List<Relation> relations = new LinkedList<Relation>();
			for (int tableIndex=0; tableIndex<10; ++tableIndex) {
				Relation newRel = RelationFactory.createSingleTableRel(query, tableIndex);
				relations.add(newRel);
			}
			assertEquals(10, relations.size());
			Set<Integer> poppedIndices = new TreeSet<Integer>();
			for (int i=0; i<10; ++i) {
				Relation poppedRel = LocalSearchUtil.popRandomRel(relations);
				assertEquals(9-i, relations.size());
				int poppedIndex = poppedRel.firstTableIndex();
				assertFalse(poppedIndices.contains(poppedIndex));
				poppedIndices.add(poppedIndex);
			}
		}
		// Test popping plans
		{
			List<Plan> plans = new LinkedList<Plan>();
			LocalScan localScan = new LocalScan();
			for (int tableIndex=0; tableIndex<10; ++tableIndex) {
				Plan newPlan = new ScanPlan(query, tableIndex, localScan);
				plans.add(newPlan);
			}
			assertEquals(10, plans.size());
			Set<Integer> poppedIndices = new TreeSet<Integer>();
			for (int i=0; i<10; ++i) {
				Plan poppedPlan = LocalSearchUtil.popRandomPlan(plans);
				assertEquals(9-i, plans.size());
				int poppedIndex = ((ScanPlan)poppedPlan).tableIndex;
				assertFalse(poppedIndices.contains(poppedIndex));
				poppedIndices.add(poppedIndex);
			}
		}
		// Test determining joined table indices
		{
			LocalScan localScan = new LocalScan();
			JoinOperator joinOp = new BNLjoin(10, true);
			Plan scan0 = new ScanPlan(query, 0, localScan);
			Plan scan3 = new ScanPlan(query, 3, localScan);
			Plan scan6 = new ScanPlan(query, 6, localScan);
			Plan join03 = new JoinPlan(query, scan0, scan3, joinOp);
			Plan join036 = new JoinPlan(query, join03, scan6, joinOp);
			Set<Integer> expectedIndices = new TreeSet<Integer>();
			expectedIndices.add(0);
			expectedIndices.add(3);
			expectedIndices.add(6);
			assertEquals(expectedIndices, LocalSearchUtil.joinedTablesIndices(join036));
		}
		// Test random generation of left-deep plans
		{
			Plan leftDeepPlan = LocalSearchUtil.randomLeftDeepPlan(query, planSpace);
			// Check that all tables joined
			Set<Integer> joinedTables = LocalSearchUtil.joinedTablesIndices(leftDeepPlan);
			Set<Integer> expectedIndices = new TreeSet<Integer>();
			for (int i=0; i<10; ++i) {
				expectedIndices.add(i);
			}
			assertEquals(expectedIndices, joinedTables);
			// Check that the plan is left-deep
			Plan currentNode = leftDeepPlan;
			for (int i=0; i<9; ++i) {
				assertTrue(currentNode instanceof JoinPlan);
				Plan rightPlan = ((JoinPlan)currentNode).getRightPlan();
				assertTrue(rightPlan instanceof ScanPlan);
				currentNode = ((JoinPlan)currentNode).getLeftPlan();
			}
		}
		// Test random generation of bushy plans
		{
			TreeSet<Integer> indicesToJoin = new TreeSet<Integer>();
			indicesToJoin.add(1);
			indicesToJoin.add(5);
			indicesToJoin.add(8);
			indicesToJoin.add(9);
			Plan bushyPlan = LocalSearchUtil.randomBushyPlan(query, planSpace, indicesToJoin);
			costModel.updateAll(bushyPlan);
			Set<Integer> joinedIndices = LocalSearchUtil.joinedTablesIndices(bushyPlan);
			assertEquals(indicesToJoin, joinedIndices);
		}
		// Test selection of appropriate join operators
		/*
		{
			JoinOperator nonMaterializingOperator = new BNLjoin(10, false);
			JoinOperator materializingOperator = new BNLjoin(10, true);
			ScanOperator localScan = new LocalScan();
			Plan scan0 = new ScanPlan(query, 0, localScan);
			Plan scan3 = new ScanPlan(query, 3, localScan);
			Plan materializingPlan = new JoinPlan(query, scan0, scan3, materializingOperator);
			Plan nonMaterializingPlan = new JoinPlan(query, scan0, scan3, nonMaterializingOperator);
			JoinOperator BNLjoin = new BNLjoin(10, false);
			JoinOperator sortMergeJoin = new SortMergeJoin(10, false);
			{
				JoinOperator selected = LocalSearchUtil.selectJoinOperator(
						materializingPlan, nonMaterializingPlan, BNLjoin, planSpace);
				assertTrue(selected instanceof BNLjoin);
			}
			{
				JoinOperator selected = LocalSearchUtil.selectJoinOperator(
						materializingPlan, nonMaterializingPlan, sortMergeJoin, planSpace);
				assertFalse(selected instanceof SortMergeJoin);
			}
			{
				JoinOperator selected = LocalSearchUtil.selectJoinOperator(
						materializingPlan, materializingPlan, sortMergeJoin, planSpace);
				assertTrue(selected instanceof SortMergeJoin);
			}
		}
		*/
		// Test plan mutations
		{
			LocalScan localScan = new LocalScan();
			JoinOperator bnlJoin = new BNLjoin(10, true);
			Plan scan0 = new ScanPlan(query, 0, localScan);
			costModel.updateRoot(scan0);
			assertTrue(scan0.materializes);
			Plan scan3 = new ScanPlan(query, 3, localScan);
			costModel.updateRoot(scan3);
			assertTrue(scan3.materializes);
			Plan scan6 = new ScanPlan(query, 6, localScan);
			costModel.updateRoot(scan6);
			assertTrue(scan6.materializes);
			Plan join03 = new JoinPlan(query, scan0, scan3, bnlJoin);
			costModel.updateRoot(join03);
			assertTrue(join03.materializes);
			Plan join036 = new JoinPlan(query, join03, scan6, bnlJoin);
			costModel.updateRoot(join036);
			assertTrue(join036.materializes);
			List<Plan> mutatedPlans = LocalSearchUtil.mutatedPlans(query, join036, planSpace, costModel);
			// Check operator mutations
			boolean haveBNLjoin = false;
			boolean haveHashJoin = false;
			boolean haveMergeJoin = false;
			for (Plan mutatedPlan : mutatedPlans) {
				JoinPlan joinPlan = (JoinPlan)mutatedPlan;
				JoinOperator joinOperator = joinPlan.getJoinOperator();
				if (joinOperator instanceof BNLjoin) {
					haveBNLjoin = true;
				}
				if (joinOperator instanceof HashJoin) {
					haveHashJoin = true;
				}
				if (joinOperator instanceof SortMergeJoin) {
					haveMergeJoin = true;
				}
			}
			assertTrue(haveBNLjoin);
			assertTrue(haveHashJoin);
			assertTrue(haveMergeJoin);
			// Check commutativity
			{
				boolean haveCommutativity = false;
				Plan commutativePlan = new JoinPlan(query, scan6, join03, bnlJoin);
				costModel.updateRoot(commutativePlan);
				for (Plan mutatedPlan : mutatedPlans) {
					if (commutativePlan.toString().equals(mutatedPlan.toString())) {
						haveCommutativity = true;
					}
				}
				assertTrue(haveCommutativity);
			}
			// Check associativity
			JoinPlan join36 = new JoinPlan(query, scan3, scan6, bnlJoin);
			costModel.updateRoot(join36);
			{
				boolean haveAssociativity = false;
				Plan associativePlan = new JoinPlan(query, scan0, join36, bnlJoin);
				costModel.updateRoot(associativePlan);
				for (Plan mutatedPlan : mutatedPlans) {
					if (associativePlan.toString().equals(mutatedPlan.toString())) {
						haveAssociativity = true;
					}
				}
				assertTrue(haveAssociativity);
			}
			// Check left join exchange
			JoinPlan join06 = new JoinPlan(query, scan0, scan6, bnlJoin);
			costModel.updateRoot(join06);
			{
				boolean haveLeftExchange = false;
				JoinPlan leftExchangePlan = new JoinPlan(query, join06, scan3, bnlJoin);
				costModel.updateRoot(leftExchangePlan);
				for (Plan mutatedPlan : mutatedPlans) {
					if (leftExchangePlan.toString().equals(mutatedPlan.toString())) {
						haveLeftExchange = true;
					}
				}
				assertTrue(haveLeftExchange);
			}
			// Check right join exchange
			{
				// Mutate new plan since former plan was left-deep
				Plan planToMutate = new JoinPlan(query, scan0, join36, bnlJoin);
				costModel.updateRoot(planToMutate);
				List<Plan> mutatedPlans2 = LocalSearchUtil.mutatedPlans(
						query, planToMutate, planSpace, costModel);
				boolean haveRightExchange = false;
				Plan rightExchangePlan = new JoinPlan(query, scan3, join06, bnlJoin);
				costModel.updateRoot(rightExchangePlan);
				for (Plan mutatedPlan : mutatedPlans2) {
					if (rightExchangePlan.toString().equals(mutatedPlan.toString())) {
						haveRightExchange = true;
					}
				}
				assertTrue(haveRightExchange);
			}
		}
		// Test join order mutations at root
		{
			double[] cardinalities = new double[] {10000, 10000, 10000};
			double[][] selectivities = TestUtil.defaultSelectivityMatrix(3);
			Query query2 = new Query(3, cardinalities, selectivities);
			ScanOperator scanOperator = planSpace.defaultScanOperator;
			JoinOperator joinOperator = planSpace.defaultJoinOperator;
			Plan scan0 = new ScanPlan(query2, 0, scanOperator);
			Plan scan1 = new ScanPlan(query2, 1, scanOperator);
			Plan scan2 = new ScanPlan(query2, 2, scanOperator);
			{
				// Test mutations for left-deep tree with three nodes
				Plan join01 = new JoinPlan(query2, scan0, scan1, joinOperator);
				Plan join012 = new JoinPlan(query2, join01, scan2, joinOperator);
				costModel.updateAll(join012);
				List<Plan> mutations = LocalSearchUtil.mutatedJoinOrders(
						query2, join012, planSpace, costModel);
				TestUtil.validatePlans(mutations, planSpace, costModel, true);
				assertEquals(3, mutations.size());
				// Examine string representations of join order
				List<String> mutatedOrders = new LinkedList<String>();
				for (Plan joinOrder : mutations) {
					mutatedOrders.add(joinOrder.orderToString());
				}
				assertEquals(3, mutatedOrders.size());
				// Check for commutativity
				assertTrue(mutatedOrders.contains("((2)((0)(1)))"));
				// Check for associativity
				assertTrue(mutatedOrders.contains("((0)((1)(2)))"));
				// Check for left join exchange
				assertTrue(mutatedOrders.contains("(((0)(2))(1))"));
			}
			{
				// Test mutations for right-deep tree with three nodes
				Plan join01 = new JoinPlan(query2, scan0, scan1, joinOperator);
				Plan join012 = new JoinPlan(query2, scan2, join01, joinOperator);
				costModel.updateAll(join012);
				List<Plan> mutations = LocalSearchUtil.mutatedJoinOrders(
						query2, join012, planSpace, costModel);
				TestUtil.validatePlans(mutations, planSpace, costModel, true);
				assertEquals(2, mutations.size());
				// Examine string representations of join order
				List<String> mutatedOrders = new LinkedList<String>();
				for (Plan joinOrder : mutations) {
					mutatedOrders.add(joinOrder.orderToString());
				}
				assertEquals(2, mutatedOrders.size());
				// Check for commutativity
				assertTrue(mutatedOrders.contains("(((0)(1))(2))"));
				// Check for right join exchange
				assertTrue(mutatedOrders.contains("((0)((2)(1)))"));
			}
		}
		// Test plan improvement
		{
			// Test case: can improve by changing join order at root (commutativity)
			{
				double[] cardinalities = new double[] {10000, 10000, 10000};
				double[][] selectivities = TestUtil.defaultSelectivityMatrix(3);
				selectivities[0][2] = 0.01;
				selectivities[2][0] = 0.01;
				Query query2 = new Query(3, cardinalities, selectivities);
				ScanOperator scanOperator = new LocalScan();
				JoinOperator joinOperator = new BNLjoin(1E6, false);
				Plan scan0 = new ScanPlan(query2, 0, scanOperator);
				Plan scan1 = new ScanPlan(query2, 1, scanOperator);
				Plan scan2 = new ScanPlan(query2, 2, scanOperator);
				costModel.updateRoot(scan0);
				costModel.updateRoot(scan1);
				costModel.updateRoot(scan2);
				Plan join01 = new JoinPlan(query2, scan0, scan1, joinOperator);
				Plan join12 = new JoinPlan(query2, scan1, scan2, joinOperator);
				Plan join02 = new JoinPlan(query2, scan0, scan2, joinOperator);
				costModel.updateRoot(join01);
				costModel.updateRoot(join12);
				costModel.updateRoot(join02);
				Plan suboptimalPlan = new JoinPlan(query2, join12, scan0, joinOperator);
				Plan betterPlan = new JoinPlan(query2, scan0, join12, joinOperator);
				costModel.updateRoot(suboptimalPlan);
				costModel.updateRoot(betterPlan);
				boolean[] onlyTimeRelevant = new boolean[] {true, false, false};
				Plan improvedPlan = LocalSearchUtil.improvedPlan(
						query2, suboptimalPlan, onlyTimeRelevant, planSpace, costModel, null);
				assertEquals(betterPlan.orderToString(), improvedPlan.orderToString());
			}
			// Test case: can improve by changing join order at root (associativity)
			{
				double[] cardinalities = new double[] {10000, 10000, 10000};
				double[][] selectivities = TestUtil.defaultSelectivityMatrix(3);
				TestUtil.setSelectivity(selectivities, 1, 2, 0.1);
				Query query2 = new Query(3, cardinalities, selectivities);
				ScanOperator scanOperator = new LocalScan();
				JoinOperator joinOperator = new SortMergeJoin(100, true);
				Plan scan0 = new ScanPlan(query2, 0, scanOperator);
				Plan scan1 = new ScanPlan(query2, 1, scanOperator);
				Plan scan2 = new ScanPlan(query2, 2, scanOperator);
				Plan join01 = new JoinPlan(query2, scan0, scan1, joinOperator);
				Plan join12 = new JoinPlan(query2, scan1, scan2, joinOperator);
				// Check cardinality
				assertEquals(1E8, join01.outputRows, EPSILON);
				assertEquals(1E7, join12.outputRows, EPSILON);
				Plan originalPlan = new JoinPlan(query2, join01, scan2, joinOperator);
				costModel.updateAll(originalPlan);
				Plan betterPlan = new JoinPlan(query2, scan0, join12, joinOperator);
				costModel.updateAll(betterPlan);
				boolean[] onlyTimeRelevant = new boolean[] {true, false, false};
				assertTrue(PruningUtil.ParetoDominates(betterPlan.getCostValuesCopy(), originalPlan.getCostValuesCopy(), onlyTimeRelevant));
				Plan improvedPlan = LocalSearchUtil.exhaustivePlanClimbing(
						query2, originalPlan, planSpace, costModel, onlyTimeRelevant, null);
				//query2, originalPlan, onlyTimeRelevant, planSpace, costModel);
				assertNotNull(improvedPlan);
				assertTrue(PruningUtil.ParetoDominates(improvedPlan.getCostValuesCopy(), originalPlan.getCostValuesCopy(), onlyTimeRelevant));
				//assertEquals(betterPlan.orderToString(), improvedPlan.orderToString());
			}
			// Test case: can improve by changing join order within tree
			{
				double[] cardinalities = new double[] {1000, 10, 1000, 1000, 1000};
				double[][] selectivities = TestUtil.defaultSelectivityMatrix(5);
				TestUtil.setSelectivity(selectivities, 0, 1, 0.1);
				TestUtil.setSelectivity(selectivities, 1, 2, 0.001);
				TestUtil.setSelectivity(selectivities, 2, 3, 0.001);
				TestUtil.setSelectivity(selectivities, 3, 4, 0.001);
				Query query2 = new Query(5, cardinalities, selectivities);
				ScanOperator scanOperator = new LocalScan();
				JoinOperator joinOperator = new BNLjoin(500, false);
				Plan scan0 = new ScanPlan(query2, 0, scanOperator);
				Plan scan1 = new ScanPlan(query2, 1, scanOperator);
				Plan scan2 = new ScanPlan(query2, 2, scanOperator);
				Plan scan3 = new ScanPlan(query2, 3, scanOperator);
				Plan scan4 = new ScanPlan(query2, 4, scanOperator);
				JoinPlan join01 = new JoinPlan(query2, scan0, scan1, joinOperator);
				JoinPlan join012 = new JoinPlan(query2, join01, scan2, joinOperator);
				JoinPlan join0123 = new JoinPlan(query2, join012, scan3, joinOperator);
				JoinPlan originalPlan = new JoinPlan(query2, join0123, scan4, joinOperator);
				costModel.updateAll(originalPlan);
				boolean[] onlyTime = new boolean[] {true, false, false};
				// Optimal plan should put the small table (index 1) at the left
				Plan locallyOptimalPlan = LocalSearchUtil.exhaustivePlanClimbing(
						query2, originalPlan, planSpace, costModel, onlyTime, null);
				assertNotNull(locallyOptimalPlan);
				assertTrue(locallyOptimalPlan instanceof JoinPlan);
				/*
				// Following assertions not guaranteed with shuffled lists of operators
				JoinPlan locallyOptimalJoinPlan = (JoinPlan)locallyOptimalPlan;
				Plan leftLocallyOptimalPlan = locallyOptimalJoinPlan.leftPlan;
				assertTrue(leftLocallyOptimalPlan instanceof ScanPlan);
				assertEquals(1, ((ScanPlan)leftLocallyOptimalPlan).tableIndex);
				*/
			}
			// Test case: can improve disc space consumption by changing operators within tree
			{
				double[] cardinalities = new double[] {1000, 10, 1000, 1000, 1000};
				double[][] selectivities = TestUtil.defaultSelectivityMatrix(5);
				Query query2 = new Query(5, cardinalities, selectivities);
				ScanOperator scanOperator = new LocalScan();
				JoinOperator joinOperator = new SortMergeJoin(1E6, true);
				Plan scan0 = new ScanPlan(query2, 0, scanOperator);
				Plan scan1 = new ScanPlan(query2, 1, scanOperator);
				Plan scan2 = new ScanPlan(query2, 2, scanOperator);
				Plan scan3 = new ScanPlan(query2, 3, scanOperator);
				Plan scan4 = new ScanPlan(query2, 4, scanOperator);
				JoinPlan join01 = new JoinPlan(query2, scan0, scan1, joinOperator);
				JoinPlan join012 = new JoinPlan(query2, join01, scan2, joinOperator);
				JoinPlan join0123 = new JoinPlan(query2, join012, scan3, joinOperator);
				JoinPlan originalPlan = new JoinPlan(query2, join0123, scan4, joinOperator);
				costModel.updateAll(originalPlan);
				boolean[] onlyDisc = new boolean[] {false, false, true};
				Plan locallyOptimalPlan = LocalSearchUtil.exhaustivePlanClimbing(
						query2, originalPlan, planSpace, costModel, onlyDisc, null);
				assertTrue(originalPlan.getCostValue(2) > locallyOptimalPlan.getCostValue(2));
			}
		}
		// Test join order improvement
		{
			double[] cardinalities = new double[] {500000, 1000000, 10000};
			double[][] selectivities = TestUtil.defaultSelectivityMatrix(3);
			Query query2 = new Query(3, cardinalities, selectivities);
			ScanOperator scanOperator = planSpace.defaultScanOperator;
			JoinOperator joinOperator = planSpace.defaultJoinOperator;
			Plan scan0 = new ScanPlan(query2, 0, scanOperator);
			Plan scan1 = new ScanPlan(query2, 1, scanOperator);
			Plan scan2 = new ScanPlan(query2, 2, scanOperator);
			JoinPlan join01 = new JoinPlan(query2, scan0, scan1, joinOperator);
			JoinPlan join012 = new JoinPlan(query2, join01, scan2, joinOperator);
			costModel.updateAll(join012);
			boolean[] consideredMetric = new boolean[] {true, true, true};
			Plan optimalJoinOrder = LocalSearchUtil.exhaustiveJoinOrderClimbing(
					query2, join012, planSpace, costModel, consideredMetric);
			TestUtil.validatePlan(optimalJoinOrder, planSpace, costModel, true);
			assertEquals("((2)((0)(1)))", optimalJoinOrder.orderToString());
		}
		{
			double[] cardinalities = new double[] {1E6, 1E6, 1E6};
			double[][] selectivities = TestUtil.defaultSelectivityMatrix(3);
			TestUtil.setSelectivity(selectivities, 1, 0, 0.0000000001);
			Query query2 = new Query(3, cardinalities, selectivities);
			ScanOperator scanOperator = planSpace.defaultScanOperator;
			JoinOperator joinOperator = planSpace.defaultJoinOperator;
			Plan scan0 = new ScanPlan(query2, 0, scanOperator);
			Plan scan1 = new ScanPlan(query2, 1, scanOperator);
			Plan scan2 = new ScanPlan(query2, 2, scanOperator);
			JoinPlan join1_0 = new JoinPlan(query2, scan1, scan0, joinOperator);
			JoinPlan join012 = new JoinPlan(query2, scan2, join1_0, joinOperator);
			costModel.updateAll(join012);
			boolean[] consideredMetric = new boolean[] {true, true, true};
			Plan optimalJoinOrder = LocalSearchUtil.exhaustiveJoinOrderClimbing(
					query2, join012, planSpace, costModel, consideredMetric);
			TestUtil.validatePlan(optimalJoinOrder, planSpace, costModel, true);
			assertEquals("(((1)(0))(2))", optimalJoinOrder.orderToString());
		}
		// Test calculating max cost delta
		{
			{
				double[] refCosts = new double[] {1, 4, 2};
				double[] testCosts = new double[] {2, 1, 3.5};
				boolean[] consideredMetric = new boolean[] {true, true, true};
				assertEquals(1.5, LocalSearchUtil.maxCostDelta(testCosts, refCosts, consideredMetric), EPSILON);
			}
			{
				double[] refCosts = new double[] {1, 1, 2};
				double[] testCosts = new double[] {0, 1, 2};
				boolean[] consideredMetric = new boolean[] {true, true, true};
				assertEquals(0, LocalSearchUtil.maxCostDelta(testCosts, refCosts, consideredMetric), EPSILON);
			}
		}
		// Test accept move
		{
			double[] dominantCost = new double[] {1, 1, 10};
			double[] dominatedCost = new double[] {2, 1, 1};
			boolean[] consideredMetric = new boolean[] {true, true, false};
			assertTrue(LocalSearchUtil.acceptMove(dominatedCost, dominantCost, consideredMetric, false, -1));
			assertTrue(LocalSearchUtil.acceptMove(dominatedCost, dominantCost, consideredMetric, true, -1));
			assertFalse(LocalSearchUtil.acceptMove(dominantCost, dominatedCost, consideredMetric, false, -1));
			assertFalse(LocalSearchUtil.acceptMove(dominantCost, dominatedCost, consideredMetric, true, 0));
			assertTrue(LocalSearchUtil.acceptMove(dominantCost, dominatedCost, consideredMetric, true, Double.POSITIVE_INFINITY));
		}
		// Test local search
		{
			Constants.TIMEOUT_MILLIS = 50;
			boolean[] consideredMetric = new boolean[] {true, true, true};
			for (int i=0; i<20; ++i) {
				Query query2 = QueryFactory.produceSteinbrunn(JoinGraphType.CHAIN, 3, JoinType.MN);
				Plan plan = LocalSearchUtil.randomBushyPlan(query2, planSpace);
				costModel.updateAll(plan);
				long startMillis = System.currentTimeMillis();
				List<Plan> mutatedPlans = LocalSearchUtil.localSearch(query2, plan, planSpace, costModel, consideredMetric, 
						5, true, 1, startMillis, 10);
				TestUtil.validatePlans(mutatedPlans, planSpace, costModel, true);
			}
		}
		// Test deep plan copy and extraction of plan nodes
		{
			double[] cardinalities = new double[] {1000, 10, 1000, 1000, 1000};
			double[][] selectivities = TestUtil.defaultSelectivityMatrix(5);
			Query query2 = new Query(5, cardinalities, selectivities);
			Plan original = LocalSearchUtil.randomBushyPlan(query2, planSpace);
			costModel.updateAll(original);
			Plan copy = original.deepMutableCopy();
			List<PathNode> originalNodes = LocalSearchUtil.planNodes(original, null, false);
			List<PathNode> copyNodes = LocalSearchUtil.planNodes(copy, null, false);
			// Plan must have exactly 9 nodes (five scan nodes, four join nodes)
			assertEquals(9, originalNodes.size());
			assertEquals(9, copyNodes.size());
			for (int i=0; i<9; ++i) {
				PathNode originalNode = originalNodes.get(i);
				PathNode copyNode = copyNodes.get(i);
				Plan originalPlan = originalNode.plan;
				Plan copyPlan = copyNode.plan;
				// Compare plan properties
				assertTrue(originalPlan instanceof ScanPlan && copyPlan instanceof ScanPlan ||
							originalPlan instanceof JoinPlan && copyPlan instanceof JoinPlan);
				assertEquals(originalPlan.materializes, copyPlan.materializes);
				assertEquals(originalPlan.outputPages, copyPlan.outputPages, EPSILON);
				assertEquals(originalPlan.outputRows, copyPlan.outputRows, EPSILON);
				assertArrayEquals(originalNode.plan.getCostValuesCopy(), copyNode.plan.getCostValuesCopy(), EPSILON);
				// Compare properties of result relation
				Relation originalRel = originalPlan.resultRel;
				Relation copyRel = copyPlan.resultRel;
				assertEquals(originalRel.cardinality, copyRel.cardinality, EPSILON);
				assertEquals(originalRel.pages, copyRel.pages, EPSILON);
				assertEquals(originalRel.tableSet, copyRel.tableSet);
				// Compare operators
				if (originalPlan instanceof ScanPlan) {
					assertEquals(((ScanPlan)originalPlan).scanOperator.toString(), 
							((ScanPlan)copyPlan).scanOperator.toString());
				} else {
					assertEquals(((JoinPlan)originalPlan).getJoinOperator().toString(), 
							((JoinPlan)copyPlan).getJoinOperator().toString());
				}
			}
		}
		// Test random moves: must yield consistent plan
		{
			for (int i=0; i<100; ++i) {
				Query query2 = QueryFactory.produceSteinbrunn(JoinGraphType.CHAIN, 3, JoinType.MN);
				Plan plan = LocalSearchUtil.randomBushyPlan(query2, planSpace);
				costModel.updateAll(plan);
				Plan mutatedPlan = LocalSearchUtil.randomMove(query2, plan, planSpace, costModel);
				TestUtil.validatePlan(mutatedPlan, planSpace, costModel, true);
			}
		}
		// (Test estimation of standard deviation)
	}

}
