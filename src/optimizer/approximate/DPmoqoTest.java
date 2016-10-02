package optimizer.approximate;

import static org.junit.Assert.*;
import static common.RandomNumbers.*;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import cost.MultiCostModel;
import cost.SingleCostModel;
import cost.local.BufferCostModel;
import cost.local.DiscCostModel;
import cost.local.TimeCostModel;
import plans.spaces.LocalPlanSpace;
import plans.spaces.PlanSpace;
import queries.JoinGraphType;
import queries.JoinType;
import queries.Query;
import queries.QueryFactory;
import util.LocalSearchUtil;
import util.ParetoUtil;
import plans.ParetoPlanSet;
import plans.Plan;

import org.junit.Test;

public class DPmoqoTest {

	@Test
	public void test() {
		{
			PlanSpace planSpace = new LocalPlanSpace();
			List<SingleCostModel> costModels = Arrays.asList(new SingleCostModel[] {
					new TimeCostModel(0), new BufferCostModel(1), new DiscCostModel(2)
			});
			MultiCostModel multiModel = new MultiCostModel(costModels);
			DPmoqo dpAlg = new DPmoqo(2);
			boolean[] consideredMetrics = new boolean[] {true, true, true};
			// Compare DP optimizer with hill climbing
			int nrJoinGraphTypes = JoinGraphType.values().length;
			for (int queryCtr=0; queryCtr<50; ++queryCtr) {
				// Generate random query
				int joinGraphIndex = random.nextInt(nrJoinGraphTypes);
				JoinGraphType joinGraph = JoinGraphType.values()[joinGraphIndex];
				int nrTables = 4 + random.nextInt(2);
				Query query = QueryFactory.produceSteinbrunn(joinGraph, nrTables, JoinType.RANDOM);
				// Generate approximation with near-optimality guarantees
				ParetoPlanSet dpResult = dpAlg.approximateParetoSet(
						query, consideredMetrics, planSpace, multiModel, null, 0, 0, 0);
				assertTrue(dpResult.plans.size()>0);
				// Make sure that near-optimality guarantees hold when comparing to random plan sample
				List<Plan> hillClimbingResults = new LinkedList<Plan>();
				Set<Integer> tableIndices = new TreeSet<Integer>();
				for (int tableIndex=0; tableIndex<nrTables; ++tableIndex) {
					tableIndices.add(tableIndex);
				}
				for (int sampleCtr=0; sampleCtr<20; ++sampleCtr) {
					Plan randomPlan = LocalSearchUtil.randomBushyPlan(query, planSpace, tableIndices);
					multiModel.updateAll(randomPlan);
					Plan localOptimum = LocalSearchUtil.exhaustivePlanClimbing(
							query, randomPlan, planSpace, multiModel, consideredMetrics, null);
					hillClimbingResults.add(randomPlan);
					hillClimbingResults.add(localOptimum);
				}
				/*
				System.out.println("DP Results:");
				for (Plan plan : dpResult.plans) {
					System.out.println(Arrays.toString(plan.cost));
					System.out.println(plan);
				}
				System.out.println("Hill Climbing Results:");
				TestUtil.showApproximation(dpResult.plans, hillClimbingResults);
				*/
				/*
				for (Plan plan : hillClimbingResults) {
					System.out.println(Arrays.toString(plan.cost));
				}
				*/
				double epsilon = ParetoUtil.epsilonError(
						dpResult.plans, hillClimbingResults, consideredMetrics);
				assertTrue(epsilon <= 1);
			}
			// Compare against hand-crafted plans
		}
	}

}
