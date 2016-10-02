package util;

import static org.junit.Assert.*;
import static util.TestUtil.*;
import plans.Plan;
import plans.ScanPlan;
import plans.operators.ScanOperator;
import plans.operators.local.LocalScan;
import queries.Query;

import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

public class ParetoUtilTest {

	@Test
	public void test() {
		boolean[] allMetrics = new boolean[] {true, true, true};
		// Calculating epsilon error between two vectors
		{
			// Worst approximation for second dimension - therefore epsilon=0.5
			double[] testedVector = new double[] {2, 3, 5};
			double[] referenceVector = new double[] {3, 2, 6};
			assertEquals(0.5, ParetoUtil.epsilonError(testedVector, 
					referenceVector, allMetrics), EPSILON);
		}
		{
			// Tested vector has lower cost than reference in all dimension; therefore the
			// the error is zero.
			double[] testedVector = new double[] {2, 2, 5};
			double[] referenceVector = new double[] {3, 2, 6};
			assertEquals(0, ParetoUtil.epsilonError(testedVector, 
					referenceVector, allMetrics), EPSILON);
		}
		// Calculating epsilon error between two vector sets
		{
			Query dummyQuery = new Query(1, new double[] {1}, new double[][]{{1}});
			ScanOperator scanOperator = new LocalScan();
			Plan testedPlan1 = new ScanPlan(dummyQuery, 0, scanOperator);
			Plan testedPlan2 = new ScanPlan(dummyQuery, 0, scanOperator);
			Plan referencePlan1 = new ScanPlan(dummyQuery, 0, scanOperator);
			Plan referencePlan2 = new ScanPlan(dummyQuery, 0, scanOperator);
			testedPlan1.setCostValues(new double[] {3, 5, 3});
			testedPlan2.setCostValues(new double[] {2, 3, 6});
			referencePlan1.setCostValues(new double[] {1, 4, 4});
			referencePlan2.setCostValues(new double[] {2, 1, 6});
			List<Plan> testedFrontier = new LinkedList<Plan>();
			List<Plan> referenceFrontier = new LinkedList<Plan>();
			referenceFrontier.add(referencePlan1);
			testedFrontier.add(testedPlan1);
			assertEquals(2, ParetoUtil.epsilonError(testedFrontier, 
					referenceFrontier, allMetrics), EPSILON);
			referenceFrontier.add(referencePlan2);
			assertEquals(4, ParetoUtil.epsilonError(testedFrontier, 
					referenceFrontier, allMetrics), EPSILON);
			testedFrontier.add(testedPlan2);
			assertEquals(2, ParetoUtil.epsilonError(testedFrontier, 
					referenceFrontier, allMetrics), EPSILON);
		}
	}

}
