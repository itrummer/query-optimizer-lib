package cost.local;

import static org.junit.Assert.*;
import static util.TestUtil.*;

import java.util.Arrays;

import common.Constants;
import cost.MultiCostModel;
import cost.SingleCostModel;
import plans.JoinPlan;
import plans.Plan;
import plans.ScanPlan;
import plans.operators.JoinOperator;
import plans.operators.ScanOperator;
import plans.operators.local.BNLjoin;
import plans.operators.local.HashJoin;
import plans.operators.local.LocalScan;
import plans.operators.local.SortMergeJoin;
import queries.Query;
import util.TestUtil;

import org.junit.Test;

public class LocalCostModelsTest {

	@Test
	public void test() {
		// Generate all local cost models
		TimeCostModel timeModel = new TimeCostModel(0);
		BufferCostModel bufferModel = new BufferCostModel(1);
		DiscCostModel discModel = new DiscCostModel(2);
		SingleCostModel[] costModels = new SingleCostModel[] {timeModel, bufferModel, discModel};
		MultiCostModel multiModel = new MultiCostModel(Arrays.asList(costModels));
		// Set constants for this test
		Constants.BYTES_PER_TUPLE = 256;
		Constants.BYTES_PER_PAGE = 1024;
		// Generate different operator configurations
		ScanOperator scanOperator = new LocalScan();
		JoinOperator bnlJoinMaterialized = new BNLjoin(100, true);
		JoinOperator bnlJoinPipelined = new BNLjoin(100, false);
		JoinOperator hashJoinMaterialized = new HashJoin(140, true);
		JoinOperator hashJoinPipelined = new HashJoin(140, false);
		JoinOperator mergeJoinMaterialized = new SortMergeJoin(10, true);
		// Verify for different queries and plans
		{
			// Join between three tables without predicates (cross product)
			double[] cardinalities = new double[] {4000, 4000, 4000};
			double[][] selectivities = TestUtil.defaultSelectivityMatrix(3);
			Query query = new Query(3, cardinalities, selectivities);
			Plan scan0 = new ScanPlan(query, 0, scanOperator);
			Plan scan1 = new ScanPlan(query, 1, scanOperator);
			Plan scan2 = new ScanPlan(query, 2, scanOperator);
			{
				// Calculate cost of left-deep pipelined BNL join plan
				Plan join01 = new JoinPlan(query, scan0, scan1, bnlJoinPipelined);
				Plan join012 = new JoinPlan(query, join01, scan2, bnlJoinPipelined);
				multiModel.updateAll(join012);
				// Check cardinalities
				assertEquals(4000, scan0.resultRel.cardinality, EPSILON);
				assertEquals(4000, scan1.resultRel.cardinality, EPSILON);
				assertEquals(4000, scan2.resultRel.cardinality, EPSILON);
				assertEquals(16E6, join01.resultRel.cardinality, EPSILON);
				assertEquals(64E9, join012.resultRel.cardinality, EPSILON);
				// Check page numbers
				assertEquals(1000, scan0.resultRel.pages, EPSILON);
				assertEquals(1000, scan1.resultRel.pages, EPSILON);
				assertEquals(1000, scan2.resultRel.pages, EPSILON);
				assertEquals(4E6, join01.resultRel.pages, EPSILON);
				assertEquals(16E9, join012.resultRel.pages, EPSILON);
				// Time cost: 1000 + (1000/100)*1000 + 4E6/100 * 1000 = 10000 + 4E7
				assertEquals(1000 + 10000 + 4E7, join012.getCostValue(0), EPSILON);
				// Buffer cost: 100 + 100
				assertEquals(200, join012.getCostValue(1), EPSILON);
				// Disc cost: consumption for base tables only
				double pages01 = scan0.outputPages + scan1.outputPages + scan2.outputPages;
				assertEquals(pages01, join012.getCostValue(2), EPSILON);
			}
			{
				// Calculate cost of left-deep BNL join plan with intermediate materialization
				Plan join01 = new JoinPlan(query, scan0, scan1, bnlJoinMaterialized);
				Plan join012 = new JoinPlan(query, join01, scan2, bnlJoinPipelined);
				multiModel.updateAll(join012);
				// Time cost: 1000 (join 1: read left input) + (1000/100)*1000 (join 1: read right) 
				// + 4E6 (join 1: write output to disc) + 4E6 (join 2: read left) 
				// + 4E6/100 * 1000 (join 2: read right)
				assertEquals(1000 + 10000 + 4E6, join01.getCostValue(0), EPSILON);
				assertEquals(1000 + 10000 + 4E6 + 4E6 + 4E7, join012.getCostValue(0), EPSILON);
				// Buffer cost: 100 (second join starts only after buffer of first join is freed)
				assertEquals(100, join012.getCostValue(1), EPSILON);
				// Disc cost: 4E6 + 3000 for storing first intermediate result and base tables
				assertEquals(4E6 + 3000, join012.getCostValue(2), EPSILON);
			}
			{
				// Calculate cost of left-deep BNL join plan with materialization always on
				Plan join01 = new JoinPlan(query, scan0, scan1, bnlJoinMaterialized);
				Plan join012 = new JoinPlan(query, join01, scan2, bnlJoinMaterialized);
				multiModel.updateAll(join012);
				// Time cost: 1000 (join 1: read left input) + (1000/100)*1000 (join 1: read right) 
				// + 4E6 (join 1: write output to disc) + 4E6 (join 2: read left) 
				// + 4E6/100 * 1000 (join 2: read right) + 16E9 (join 2: write out)
				assertEquals(1000 + 10000 + 4E6, join01.getCostValue(0), EPSILON);
				assertEquals(1000 + 10000 + 4E6 + 4E6 + 4E7 + 16E9, join012.getCostValue(0), EPSILON);
				// Buffer cost: 100 (second join starts only after buffer of first join is freed)
				assertEquals(100, join012.getCostValue(1), EPSILON);
				// Disc cost: 4E6 (first intermediate result) + 16E9 (result) + base tables
				double baseTablesDisc = scan0.outputPages + scan1.outputPages + scan2.outputPages;
				assertEquals(4E6 + 16E9 + baseTablesDisc, join012.getCostValue(2), EPSILON);
			}
			{
				// Check whether fraction q for hash join is correctly calculated -
				// higher comparison tolerance since Steinbrunn formulas take into 
				// account buffer space reservations for output.
				assertEquals(0.1, timeModel.calculateTableFraction(1000, 100), 0.02);
				assertEquals(0.5, timeModel.calculateTableFraction(1000, 500), 0.02);
				assertEquals(0.75, timeModel.calculateTableFraction(1000, 750), 0.02);
			}
			{
				// Calculate cost of left-deep hash join plan with materialization
				Plan join01 = new JoinPlan(query, scan0, scan1, hashJoinMaterialized);
				Plan join012 = new JoinPlan(query, join01, scan2, hashJoinMaterialized);
				multiModel.updateAll(join012);
				// Time cost first join: 1000 (read left) + 1000 (read right) 
				// + 2 * (1000 + 1000) * (1-q) (join itself) where q = 0.1 
				// + 4E6 (writing output)
				double qJoin1 = timeModel.calculateTableFraction(1000, 140);
				double join1ExpectedCost = 1000 + 1000 + 2 * 2000 * (1.0-qJoin1) + 4E6;
				assertEquals(join1ExpectedCost, join01.getCostValue(0), EPSILON);
				// Time cost second join: cost of first join + 4E6 (read left) + 1000 (read right)
				// + 2 * (4E6 + 1000) * (1-qJoin2) + 16E9 (write result)
				double qJoin2 = timeModel.calculateTableFraction(4E6, 140);
				double join2ExpectedCost = join1ExpectedCost + 4E6 + 1000 + 2*(4E6+1000)*(1-qJoin2) + 16E9;
				assertEquals(join2ExpectedCost, join012.getCostValue(0), EPSILON);
				// Buffer space cost: 140
				assertEquals(140, join01.getCostValue(1), EPSILON);
				assertEquals(140, join012.getCostValue(1), EPSILON);
				// Disc space cost: 4E6 + 16E9
				double tables01Disc = scan0.outputPages + scan1.outputPages;
				double tables012Disc = scan0.outputPages + scan1.outputPages + scan2.outputPages;
				assertEquals(4E6 + tables01Disc, join01.getCostValue(2), EPSILON);
				assertEquals(4E6 + 16E9 + tables012Disc, join012.getCostValue(2), EPSILON);
			}
			{
				// Calculate cost of right-deep hash-BNL join plan
				Plan join12 = new JoinPlan(query, scan1, scan2, hashJoinPipelined);
				Plan join012 = new JoinPlan(query, scan0, join12, bnlJoinPipelined);
				multiModel.updateAll(join012);
				// Time cost of first join: 1000 + 1000 + 2*(1000+1000)*(1-q)
				double q = timeModel.calculateTableFraction(1000, 140);
				double join1ExpectedTime = 1000 + 1000 + 2*(1000+1000)*(1-q);
				assertEquals(join1ExpectedTime, join12.getCostValue(0), EPSILON);
				// Time cost of second join: 1000 (read left) 
				// + 1000/100 * join1ExpectedCost (read right repeatedly)
				double totalExpectedTime = 1000 + 10 * join1ExpectedTime;
				assertEquals(totalExpectedTime, join012.getCostValue(0), EPSILON);
				// Buffer space: 100 + 140
				assertEquals(100 + 140, join012.getCostValue(1), EPSILON);
				// Disc space: only base tables (since no intermediate results are materialized).
				double baseTablesDisc = scan0.outputPages + scan1.outputPages + scan2.outputPages;
				assertEquals(baseTablesDisc, join012.getCostValue(2), EPSILON);
			}
			{
				// Calculate cost of sort-merge join plan
				Plan join12 = new JoinPlan(query, scan1, scan2, mergeJoinMaterialized);
				Plan join012 = new JoinPlan(query, scan0, join12, bnlJoinPipelined);
				multiModel.updateAll(join012);
				// Time of first join: 1000 (left read) + 1000 (right read) 
				// + 1000 * log_10(1000) (left sort) + 1000 * log_10(1000) (right sort)
				// + 4E6 (writing output)
				double join1ExpectedTime = 1000 + 1000 + 1000 * 3 + 1000 * 3 + 4E6;
				assertEquals(join1ExpectedTime, join12.getCostValue(0), EPSILON);
				// Buffer for first join: 10
				assertEquals(10, join12.getCostValue(1), EPSILON);
				// Disc space for first join: 4E6 + base table disc
				double baseTables12Disc = scan1.outputPages + scan2.outputPages;
				assertEquals(4E6 + baseTables12Disc, join12.getCostValue(2), EPSILON);
				// Time of second join: 1000 (left read) + 1000/100 * 4E6 (right read) + join 1 time
				double join2ExpectedTime = 1000 + 10 * 4E6 + join1ExpectedTime;
				assertEquals(join2ExpectedTime, join012.getCostValue(0), EPSILON);
				// Buffer for second join: 100
				assertEquals(100, join012.getCostValue(1), EPSILON);
				// Disc space for second join: 4E6
				double baseTables012Disc = scan0.outputPages + scan1.outputPages + scan2.outputPages;
				assertEquals(4E6 + baseTables012Disc, join012.getCostValue(2), EPSILON);
			}
		}
	}

}
