package util;

import static org.junit.Assert.*;
import static util.TestUtil.*;

import java.util.BitSet;
import java.util.List;

import plans.JoinPlan;
import plans.Plan;
import plans.ScanPlan;
import queries.Query;

import org.junit.Test;

public class GreedyUtilTest {

	@Test
	public void test() {
		// Deciding which of two plans is better
		{
			// Create query
			double[] tableCardinalities = new double[] {100, 10000, 100000};
			double[][] selectivities = TestUtil.defaultSelectivityMatrix(3);
			TestUtil.setSelectivity(selectivities, 1, 2, 0.1);
			Query query = new Query(3, tableCardinalities, selectivities);
			// Create scan plans
			ScanPlan scan0 = new ScanPlan(query, 0, planSpace.defaultScanOperator);
			ScanPlan scan1 = new ScanPlan(query, 1, planSpace.defaultScanOperator);
			ScanPlan scan2 = new ScanPlan(query, 2, planSpace.defaultScanOperator);
			// Create join plans
			JoinPlan join01 = new JoinPlan(query, scan0, scan1, planSpace.defaultJoinOperator);
			JoinPlan join10 = new JoinPlan(query, scan1, scan0, planSpace.defaultJoinOperator);
			JoinPlan join12 = new JoinPlan(query, scan1, scan2, planSpace.defaultJoinOperator);
			JoinPlan join21 = new JoinPlan(query, scan2, scan1, planSpace.defaultJoinOperator);
			// Minimum selectivity criterion
			{
				// The join between the second and third table leads to minimal selectivity
				assertTrue(GreedyUtil.better(join12, join01, GreedyCriterion.MIN_SELECTIVITY));
				assertTrue(GreedyUtil.better(join12, join10, GreedyCriterion.MIN_SELECTIVITY));
				assertTrue(GreedyUtil.better(join21, join01, GreedyCriterion.MIN_SELECTIVITY));
				assertTrue(GreedyUtil.better(join21, join10, GreedyCriterion.MIN_SELECTIVITY));
				
				assertFalse(GreedyUtil.better(join01, join12, GreedyCriterion.MIN_SELECTIVITY));
				assertFalse(GreedyUtil.better(join10, join12, GreedyCriterion.MIN_SELECTIVITY));
				assertFalse(GreedyUtil.better(join01, join21, GreedyCriterion.MIN_SELECTIVITY));
				assertFalse(GreedyUtil.better(join10, join21, GreedyCriterion.MIN_SELECTIVITY));
				
				assertFalse(GreedyUtil.better(join10, join10, GreedyCriterion.MIN_SELECTIVITY));
				assertFalse(GreedyUtil.better(join01, join10, GreedyCriterion.MIN_SELECTIVITY));
				assertFalse(GreedyUtil.better(join12, join12, GreedyCriterion.MIN_SELECTIVITY));
				assertFalse(GreedyUtil.better(join21, join12, GreedyCriterion.MIN_SELECTIVITY));
			}
			// Minimum size criterion
			{
				// The join between the first and the second table leads to minimal result size
				assertFalse(GreedyUtil.better(join12, join01, GreedyCriterion.MIN_SIZE));
				assertFalse(GreedyUtil.better(join12, join10, GreedyCriterion.MIN_SIZE));
				assertFalse(GreedyUtil.better(join21, join01, GreedyCriterion.MIN_SIZE));
				assertFalse(GreedyUtil.better(join21, join10, GreedyCriterion.MIN_SIZE));
				
				assertTrue(GreedyUtil.better(join01, join12, GreedyCriterion.MIN_SIZE));
				assertTrue(GreedyUtil.better(join10, join12, GreedyCriterion.MIN_SIZE));
				assertTrue(GreedyUtil.better(join01, join21, GreedyCriterion.MIN_SIZE));
				assertTrue(GreedyUtil.better(join10, join21, GreedyCriterion.MIN_SIZE));
				
				assertFalse(GreedyUtil.better(join10, join10, GreedyCriterion.MIN_SIZE));
				assertFalse(GreedyUtil.better(join01, join10, GreedyCriterion.MIN_SIZE));
				assertFalse(GreedyUtil.better(join12, join12, GreedyCriterion.MIN_SIZE));
				assertFalse(GreedyUtil.better(join21, join12, GreedyCriterion.MIN_SIZE));

			}
		}
		// Selecting the minSize join
		{
			{
				// Create query
				double[] tableCardinalities = new double[] {100, 10000, 100000};
				double[][] selectivities = TestUtil.defaultSelectivityMatrix(3);
				Query query = new Query(3, tableCardinalities, selectivities);
				// Create all scan plans for the query
				List<Plan> partialPlans = query.allScanPlans(planSpace, timeCostModel);
				// Insert minSize join
				GreedyUtil.performGreedyJoin(query, partialPlans, planSpace, 
						timeCostModel, GreedyCriterion.MIN_SIZE);
				// After inserting a new join plan and removing its sub-plans the list size must be two
				assertEquals(2, partialPlans.size());
				// The list needs to contain a join of the first two tables
				boolean performedRightJoin = false;
				BitSet joinResultIndices = new BitSet();
				joinResultIndices.set(0);
				joinResultIndices.set(1);
				for (Plan plan : partialPlans) {
					if (plan.resultRel.tableSet.equals(joinResultIndices)) {
						performedRightJoin = true;
					}
				}
				assertTrue(performedRightJoin);
			}
			{
				// Create query
				double[] tableCardinalities = new double[] {1000000, 10000, 100000};
				double[][] selectivities = TestUtil.defaultSelectivityMatrix(3);
				Query query = new Query(3, tableCardinalities, selectivities);
				// Create all scan plans for the query
				List<Plan> partialPlans = query.allScanPlans(planSpace, timeCostModel);
				// Insert minSize join
				GreedyUtil.performGreedyJoin(query, partialPlans, 
						planSpace, timeCostModel, GreedyCriterion.MIN_SIZE);
				// After inserting a new join plan and removing its sub-plans the list size must be two
				assertEquals(2, partialPlans.size());
				// The list needs to contain a join of the first two tables
				boolean performedRightJoin = false;
				BitSet joinResultIndices = new BitSet();
				joinResultIndices.set(1);
				joinResultIndices.set(2);
				for (Plan plan : partialPlans) {
					if (plan.resultRel.tableSet.equals(joinResultIndices)) {
						performedRightJoin = true;
					}
				}
				assertTrue(performedRightJoin);
			}
		}
		// Creating a minSize plan
		{
			// Create query
			double[] tableCardinalities = new double[] {100, 10000, 100000};
			double[][] selectivities = TestUtil.defaultSelectivityMatrix(3);
			Query query = new Query(3, tableCardinalities, selectivities);
			Plan plan = GreedyUtil.greedyPlan(query, planSpace, 
					timeCostModel, GreedyCriterion.MIN_SIZE);
			assertTrue(plan.orderToString().contains("((0)(1))") || 
					plan.orderToString().contains("((1)(0))"));
			TestUtil.validatePlan(plan, planSpace, timeCostModel, true);
		}
		{
			// Create query
			double[] tableCardinalities = new double[] {100000000, 10000, 100000};
			double[][] selectivities = TestUtil.defaultSelectivityMatrix(3);
			Query query = new Query(3, tableCardinalities, selectivities);
			Plan plan = GreedyUtil.greedyPlan(query, planSpace, 
					timeCostModel, GreedyCriterion.MIN_SIZE);
			assertTrue(plan.orderToString().contains("((1)(2))") || 
					plan.orderToString().contains("((2)(1))"));
			TestUtil.validatePlan(plan, planSpace, timeCostModel, true);
		}
	}

}
