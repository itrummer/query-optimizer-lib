package util;

import static common.Constants.*;
import cost.MultiCostModel;
import cost.SingleCostModel;
import cost.local.BufferCostModel;
import cost.local.DiscCostModel;
import cost.local.TimeCostModel;
import plans.JoinPlan;
import plans.Plan;
import plans.ScanPlan;
import plans.operators.JoinOperator;
import plans.spaces.LocalPlanSpace;
import plans.spaces.PlanSpace;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

// Utility functions for performing JUnit tests.
public class TestUtil {
	public final static double EPSILON = 1E-10;
	public final static double LARGE_EPSILON = 1E-4;
	// Standard local cost model
	public final static PlanSpace planSpace = new LocalPlanSpace();
	public final static List<SingleCostModel> costModels = 
			Arrays.asList(new SingleCostModel[] {
					new TimeCostModel(0), new BufferCostModel(1), new DiscCostModel(2)});
	public final static MultiCostModel costModel = new MultiCostModel(costModels);
	public final static MultiCostModel timeCostModel = new MultiCostModel(
			Arrays.asList(new SingleCostModel[] {new TimeCostModel(0)}));

	// Obtain a selectivity matrix of specified dimensions that is initialized with 1.
	public static double[][] defaultSelectivityMatrix(int nrTables) {
		double[][] matrix = new double[nrTables][nrTables];
		for (int table1=0; table1<nrTables; ++table1) {
			for (int table2=0; table2<nrTables; ++table2) {
				matrix[table1][table2] = 1.0;
			}
		}
		return matrix;
	}
	// Set selectivity between specified tables.
	public static void setSelectivity(
			double[][] selectivityMatrix, int table1, int table2, double selectivity) {
		assert(table1 != table2);
		selectivityMatrix[table1][table2] = selectivity;
		selectivityMatrix[table2][table1] = selectivity;
	}
	// Outputs reference set and shows approximation error for each entry.
	public static void showApproximation(List<Plan> testedFrontier, 
			List<Plan> referenceFrontier, boolean[] consideredMetric) {
		for (Plan refPlan : referenceFrontier) {
			System.out.println("Reference plan with " + Arrays.toString(refPlan.getCostValuesCopy()) + ":\t" + refPlan);
			// How well is this cost vector approximated?
			double epsilon = Double.POSITIVE_INFINITY;
			Plan bestPlan = null;
			for (Plan testPlan : testedFrontier) {
				double curEpsilon = ParetoUtil.epsilonError(testPlan.getCostValuesCopy(), 
						refPlan.getCostValuesCopy(), consideredMetric);
				if (curEpsilon<epsilon) {
					bestPlan = testPlan;
				}
				epsilon = Math.min(epsilon, curEpsilon);
			}
			System.out.println("Epsilon: " + epsilon);
			System.out.println("Closest plan with " + Arrays.toString(bestPlan.getCostValuesCopy()) + ":\t" + bestPlan);
		}
	}
	// Recalculate cost of query plan and assert that it is the same as before.
	public static void validateCost(Plan plan, MultiCostModel costModel) {
		assert(plan.getCostValuesCopy().length == NR_COST_METRICS);
		Plan planCopy = plan.deepMutableCopy();
		costModel.updateAll(planCopy);
		double[] originalCost = plan.getCostValuesCopy();
		double[] recalculatedCost = planCopy.getCostValuesCopy();
		boolean consistentCost = true;
		for (int metricCtr=0; metricCtr<NR_COST_METRICS; ++metricCtr) {
			if (originalCost[metricCtr] > recalculatedCost[metricCtr] + recalculatedCost[metricCtr] * LARGE_EPSILON ||
					originalCost[metricCtr] < recalculatedCost[metricCtr] - recalculatedCost[metricCtr] * LARGE_EPSILON) {
				consistentCost = false;
			}
		}
		assert consistentCost : 
			"Original: " + Arrays.toString(originalCost) + 
			"; Recalculated: " + Arrays.toString(recalculatedCost)
			+ "; Plan: " + plan.toString() + "; Order: " + plan.orderToString();
	}
	// Validate that each operator in the plan is applicable.
	public static void validateOperators(Plan plan, PlanSpace planSpace) {
		if (plan instanceof ScanPlan) {
			assert(planSpace.scanOperatorApplicable(((ScanPlan)plan).scanOperator, plan.resultRel));
		} else {
			assert(plan instanceof JoinPlan);
			JoinPlan joinPlan = (JoinPlan)plan;
			JoinOperator operator = joinPlan.getJoinOperator();
			assert(planSpace.joinOperatorApplicable(
					operator, joinPlan.getLeftPlan(), joinPlan.getRightPlan()));
			validateOperators(joinPlan.getLeftPlan(), planSpace);
			validateOperators(joinPlan.getRightPlan(), planSpace);
		}
	}
	// Validates that the plan joins precisely the tables of its result relation.
	public static BitSet validateJoinedTables(Plan plan) {
		BitSet actualTablesRead = new BitSet();
		if (plan instanceof ScanPlan) {
			ScanPlan scanPlan = (ScanPlan)plan;
			actualTablesRead.set(scanPlan.tableIndex);
		} else {
			assert(plan instanceof JoinPlan);
			JoinPlan joinPlan = (JoinPlan)plan;
			BitSet actualTablesReadLeft = validateJoinedTables(joinPlan.getLeftPlan());
			BitSet actualTablesReadRight = validateJoinedTables(joinPlan.getRightPlan());
			actualTablesRead.or(actualTablesReadLeft);
			actualTablesRead.or(actualTablesReadRight);
		}
		BitSet claimedTablesRead = plan.resultRel.tableSet;
		if (claimedTablesRead != null) {
			assert(claimedTablesRead.equals(actualTablesRead));
		}
		return actualTablesRead;
	}
	// Validates one plan with all available tests
	public static void validatePlan(Plan plan, PlanSpace planSpace, 
			MultiCostModel costModel, boolean checkJoinedTables) {
		validateCost(plan, costModel);
		validateOperators(plan, planSpace);
		if (checkJoinedTables) {
			validateJoinedTables(plan);			
		}
	}
	// Validates all plans in the test with all available tests
	public static void validatePlans(List<Plan> plans, PlanSpace planSpace, 
			MultiCostModel costModel, boolean checkJoinedTables) {
		for (Plan plan : plans) {
			validatePlan(plan, planSpace, costModel, checkJoinedTables);
		}
	}
	// Returns true if both plans join the same tables or both have a null pointer as result relation.
	public static boolean joinSameTables(Plan plan1, Plan plan2) {
		return (plan1.resultRel == null && plan2.resultRel == null ||
				plan1.resultRel.tableSet.equals(plan2.resultRel.tableSet));
	}
}
