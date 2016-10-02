package plans.spaces;

import static org.junit.Assert.*;
import plans.JoinPlan;
import plans.Plan;
import plans.ScanPlan;
import plans.operators.JoinOperator;
import plans.operators.ScanOperator;
import plans.operators.local.BNLjoin;
import plans.operators.local.HashJoin;
import plans.operators.local.LocalScan;
import plans.operators.local.SortMergeJoin;
import queries.JoinGraphType;
import queries.JoinType;
import queries.Query;
import queries.QueryFactory;
import relations.Relation;
import relations.RelationFactory;

import org.junit.Test;

public class LocalPlanSpaceTest {

	@Test
	public void test() {
		PlanSpace localPlanSpace = new LocalPlanSpace();
		Query query = QueryFactory.produceSteinbrunn(JoinGraphType.STAR, 3, JoinType.MN);
		Relation rel0 = RelationFactory.createSingleTableRel(query, 0);
		ScanOperator scanOp = new LocalScan();
		JoinOperator materializingBNL = new BNLjoin(100, true);
		JoinOperator pipeliningBNL = new BNLjoin(100, false);
		JoinOperator sortMerge = new SortMergeJoin(100, true);
		JoinOperator hashJoin = new HashJoin(50, false);
		Plan scan0 = new ScanPlan(query, 0, scanOp);
		Plan scan1 = new ScanPlan(query, 1, scanOp);
		Plan scan2 = new ScanPlan(query, 2, scanOp);
		Plan join01pipelined = new JoinPlan(query, scan0, scan1, pipeliningBNL);
		Plan join01materialized = new JoinPlan(query, scan0, scan1, materializingBNL);
		// Test operator applicability (depending on input properties)
		{
			assertTrue(localPlanSpace.scanOperatorApplicable(scanOp, rel0));
			assertTrue(localPlanSpace.joinOperatorApplicable(materializingBNL, join01pipelined, scan2));
			assertTrue(localPlanSpace.joinOperatorApplicable(pipeliningBNL, join01pipelined, scan2));
			assertFalse(localPlanSpace.joinOperatorApplicable(sortMerge, join01pipelined, scan2));
			assertFalse(localPlanSpace.joinOperatorApplicable(hashJoin, join01pipelined, scan2));
			assertTrue(localPlanSpace.joinOperatorApplicable(sortMerge, join01materialized, scan2));
			assertTrue(localPlanSpace.joinOperatorApplicable(hashJoin, join01materialized, scan2));
		}
		// Test operator compatibility (with required output properties)
		{
			assertTrue(localPlanSpace.scanOutputCompatible(scanOp, hashJoin));
			// All pairs of BNL operators are compatible
			assertTrue(localPlanSpace.joinOutputComptatible(pipeliningBNL, pipeliningBNL));
			assertTrue(localPlanSpace.joinOutputComptatible(materializingBNL, pipeliningBNL));
			assertTrue(localPlanSpace.joinOutputComptatible(materializingBNL, materializingBNL));
			assertTrue(localPlanSpace.joinOutputComptatible(pipeliningBNL, materializingBNL));
			// Hash and sort merge join require materialized input
			assertFalse(localPlanSpace.joinOutputComptatible(pipeliningBNL, hashJoin));
			assertFalse(localPlanSpace.joinOutputComptatible(pipeliningBNL, sortMerge));
			assertTrue(localPlanSpace.joinOutputComptatible(materializingBNL, hashJoin));
			assertTrue(localPlanSpace.joinOutputComptatible(materializingBNL, sortMerge));
		}
	}

}
