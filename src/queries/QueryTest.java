package queries;

import static org.junit.Assert.*;
import static util.TestUtil.*;
import plans.Plan;
import plans.ScanPlan;

import java.util.BitSet;
import java.util.List;

import org.junit.Test;

public class QueryTest {

	@Test
	public void test() {
		// Generation of all scan plans
		Query query = QueryFactory.produceSteinbrunn(JoinGraphType.STAR, 10, JoinType.MIN);
		List<Plan> allScanPlans = query.allScanPlans(planSpace, timeCostModel);
		assertEquals(10, allScanPlans.size());
		// Create expected set of scanned table indices
		BitSet expectedScannedTables = new BitSet();
		for (int tableIndex=0; tableIndex<10; ++tableIndex) {
			expectedScannedTables.set(tableIndex);
		}
		// Create set of actually scanned table indices
		BitSet scannedTables = new BitSet();
		for (Plan plan : allScanPlans) {
			ScanPlan scanPlan = (ScanPlan)plan;
			scannedTables.set(scanPlan.tableIndex);
		}
		// Compare both sets
		assertEquals(expectedScannedTables, scannedTables);
	}

}
