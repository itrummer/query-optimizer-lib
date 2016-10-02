package util;

import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.List;

import plans.JoinPlan;
import plans.Plan;
import plans.ScanPlan;
import plans.operators.JoinOperator;
import plans.operators.ScanOperator;
import plans.operators.local.BNLjoin;
import plans.operators.local.LocalScan;
import queries.JoinGraphType;
import queries.JoinType;
import queries.Query;
import queries.QueryFactory;

import org.junit.Test;

public class PruningUtilTest {

	@Test
	public void test() {
		// Approximation for single cost metric
		{
			assertTrue(PruningUtil.approximates(10, 11, 1.1));
			assertTrue(PruningUtil.approximates(11, 10, 1.1));
			assertFalse(PruningUtil.approximates(12, 10, 1.1));
		}
		// Approximation for multiple cost metrics
		{
			{
				double[] v1 = new double[] {11, 11, 11};
				double[] v2 = new double[] {10, 10, 10};
				boolean[] consideredMetric = new boolean[] {true, true, true};
				assertTrue(PruningUtil.approximatelyDominates(v1, v2, 1.1, consideredMetric));
				assertFalse(PruningUtil.approximatelyDominates(v1, v2, 1, consideredMetric));
			}
			{
				double[] v1 = new double[] {10, 11, 10};
				double[] v2 = new double[] {10, 10, 10};
				boolean[] consideredMetric = new boolean[] {true, false, true};
				assertTrue(PruningUtil.approximatelyDominates(v1, v2, 1.1, consideredMetric));
				assertTrue(PruningUtil.approximatelyDominates(v1, v2, 1, consideredMetric));
			}
		}
		// Check for Pareto dominance
		{
			{
				double[] v1 = new double[] {10, 10, 10};
				double[] v2 = new double[] {10, 10, 10};
				boolean[] consideredMetric = new boolean[] {true, true, true};
				assertFalse(PruningUtil.ParetoDominates(v1, v2, consideredMetric));
			}
			{
				double[] v1 = new double[] {10, 9, 10};
				double[] v2 = new double[] {10, 10, 10};
				boolean[] consideredMetric = new boolean[] {true, true, true};
				assertTrue(PruningUtil.ParetoDominates(v1, v2, consideredMetric));
			}
			{
				double[] v1 = new double[] {10, 9, 10};
				double[] v2 = new double[] {10, 10, 10};
				boolean[] consideredMetric = new boolean[] {true, false, true};
				assertFalse(PruningUtil.ParetoDominates(v1, v2, consideredMetric));
			}
			{
				double[] v1 = new double[] {8, 9, 12};
				double[] v2 = new double[] {10, 10, 10};
				boolean[] consideredMetric = new boolean[] {true, true, true};
				assertFalse(PruningUtil.ParetoDominates(v1, v2, consideredMetric));
			}
			{
				double[] v1 = new double[] {10, 10, 10};
				double[] v2 = new double[] {10, 10, 10};
				boolean[] consideredMetric = new boolean[] {true, true, true};
				assertFalse(PruningUtil.ParetoDominates(v1, v2, consideredMetric));
			}
		}
		// Comparison of output properties
		{
			{
				Query query = QueryFactory.produce(JoinGraphType.CHAIN, 10, 100000, JoinType.MN);
				ScanOperator scanOp = new LocalScan();
				Plan scan0 = new ScanPlan(query, 0, scanOp);
				Plan scan1 = new ScanPlan(query, 1, scanOp);
				JoinOperator materializedJoinOp = new BNLjoin(10, true);
				JoinOperator nonMaterializedJoinOp = new BNLjoin(10, false);
				Plan joinMaterialized = new JoinPlan(query, scan0, scan1, materializedJoinOp);
				Plan joinNonMaterialized = new JoinPlan(query, scan0, scan1, nonMaterializedJoinOp);
				assertTrue(PruningUtil.sameOutputProperties(joinMaterialized, joinMaterialized));
				assertFalse(PruningUtil.sameOutputProperties(joinMaterialized, joinNonMaterialized));
			}
		}
		// Cost-based pruning
		{
			{
				Query query = QueryFactory.produce(JoinGraphType.CHAIN, 10, 100000, JoinType.MN);
				ScanOperator scanOp = new LocalScan();
				Plan plan1 = new ScanPlan(query, 0, scanOp);
				Plan plan2 = new ScanPlan(query, 0, scanOp);
				Plan plan3 = new ScanPlan(query, 0, scanOp);
				Plan plan4 = new ScanPlan(query, 0, scanOp);
				plan1.setCostValues(new double[] {1, 2, 0});
				plan2.setCostValues(new double[] {3, 2, 0});
				plan3.setCostValues(new double[] {1, 1, 0});
				plan4.setCostValues(new double[] {3, 0.5, 0});
				boolean[] consideredMetric = new boolean[] {true, true, true};
				List<Plan> plans = new LinkedList<Plan>();
				plans.add(plan1);
				assertEquals(1, plans.size());
				PruningUtil.pruneCostBased(plans, plan2, consideredMetric);
				assertEquals(1, plans.size());	// new plan was dominated
				PruningUtil.pruneCostBased(plans, plan3, consideredMetric);
				assertEquals(1, plans.size());	// new plan dominated old one
				PruningUtil.pruneCostBased(plans, plan4, consideredMetric);
				assertEquals(2, plans.size());	// new plan and old plan both Pareto-optimal
			}
		}
	}

}
