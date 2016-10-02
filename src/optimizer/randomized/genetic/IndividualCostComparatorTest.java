package optimizer.randomized.genetic;

import static util.TestUtil.planSpace;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;

import cost.MultiCostModel;
import cost.SingleCostModel;
import cost.local.TimeCostModel;
import queries.JoinGraphType;
import queries.JoinType;
import queries.Query;
import queries.QueryFactory;

import org.junit.Test;

public class IndividualCostComparatorTest {

	@Test
	public void test() {
		MultiCostModel costModel = new MultiCostModel(
				Arrays.asList(new SingleCostModel[] {new TimeCostModel(0)}));
		boolean[] consideredMetrics = new boolean[] {true};
		Query query = QueryFactory.produceSteinbrunn(JoinGraphType.CHAIN, 50, JoinType.MIN);
		SoqoGA ga = new SoqoGA();
		ga.init(query, consideredMetrics, planSpace, costModel);
		// Sort population by cost value using tested comparator
		Collections.sort(ga.population, new IndividualCostComparator(0));
		// Make sure that individuals are indeed ordered in ascending order of cost
		for (int i=0; i<127; ++i) {
			assertTrue(ga.population.get(i).cost[0] <= ga.population.get(i+1).cost[0]);
		}
	}

}
