package benchmark;

import static common.RandomNumbers.*;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import common.Constants;
import cost.MultiCostModel;
import cost.SingleCostModel;
import cost.local.BufferCostModel;
import cost.local.DiscCostModel;
import cost.local.TimeCostModel;
import optimizer.Optimizer;
import optimizer.approximate.DPmoqo;
import optimizer.randomized.genetic.NSGA2;
import optimizer.randomized.moqo.AnnealingPhaseSAIO;
import optimizer.randomized.moqo.ClimbingPhase;
import optimizer.randomized.moqo.FastClimber;
import optimizer.randomized.moqo.OnePhase;
import optimizer.randomized.moqo.TwoPhase;
import optimizer.randomized.moqo.X;
import plans.Plan;
import plans.ParetoPlanSet;
import plans.spaces.LocalPlanSpace;
import plans.spaces.LocalSpaceVariant;
import plans.spaces.PlanSpace;
import queries.JoinGraphType;
import queries.JoinType;
import queries.Query;
import queries.QueryFactory;
import util.PruningUtil;
import util.TestUtil;

/**
 * Runs a comparison between different multi-objective query optimization algorithms.
 * The benchmark is configured via various static fields that allow to specify join graph
 * structure, query sizes, the set of compared algorithms etc. At the end of the benchmark,
 * average values are displayed for various performance counters.
 * 
 * @author immanueltrummer
 *
 */
public class RunMoqoBenchmarkExact {
	/**
	 * Query size: how many tables are joined
	 */
	final static int nrQuerySizes = 1;
	/**
	 * Offset for query table number
	 */
	final static int querySizeOffset = 8;
	/**
	 * Number of table delta between query sizes
	 */
	final static int querySizeStep = 1;
	/**
	 * Number of test cases over which we average performance metrics
	 */
	final static int nrTestCasesPerConf = 10;
	/**
	 * Maximal query table cardinality
	 */
	final static double maxTableCardinality = 100000;
	/**
	 * The plan space decides which operators to consider
	 */
	//final static PlanSpace planSpace = new ClusterPlanSpace();
	final static PlanSpace planSpace = new LocalPlanSpace(LocalSpaceVariant.MOQO);
	/**
	 * Which cost metrics to calculate on query plans
	 */
	/*
	final static MultiCostModel costModel =
			new MultiCostModel(Arrays.asList(new SingleCostModel[] {
					new ClusterTimeCostModel(0), new ClusterNodesCostModel(1)}));
	*/
	final static MultiCostModel costModel =
			new MultiCostModel(Arrays.asList(new SingleCostModel[] {
					new TimeCostModel(0), new BufferCostModel(1), new DiscCostModel(2)}));
	/**
	 * Benchmarked optimization algorithms
	 */
	final static Optimizer[] optimizers = new Optimizer[] {
		new DPmoqo(1.01),
		/*
		new DPmoqo(Double.POSITIVE_INFINITY),
		new DPmoqo(1000),
		//new DPmoqo(10),
		new DPmoqo(2),
		//new OnePhase(new ClimbingPhase(true, -1)),
		//new OnePhase(new ClimbingPhase(false, 10)),
		new OnePhase(new AnnealingPhaseSAIO(1)),
		new OnePhase(new AnnealingPhaseSAIO(10)),
		new TwoPhase(10, new ClimbingPhase(true, -1), new AnnealingPhaseSAIO(1, 0.1)),
		//new GeneticAlgorithm(100, 0.9),
		new NSGA2(200, 0.9),
		//new GeneticAlgorithm(500, 0.9),
		//new GeneticAlgorithmExtended(200, 0.9)
		//new UCBpartialPlanSampling(),
		//new UCBpartialPlanSampling2(),
		//new Xwonder(),
		new FastClimber(true),
		new X(),
		//new FastClimber(false),
		*/
	};
	final static int nrOptimizers = optimizers.length;
	// Get number of tables for query size index
	static int nrQueryTables(int querySizeIndex) {
		return querySizeOffset + querySizeIndex * querySizeStep;
	}
	/**
	 * Randomly selects the specified number of cost metrics and returns appropriate vector.
	 * 
	 * @return a Boolean vector describing which cost metrics are enabled
	 */
	static boolean[] randomMetrics(int nrEnabledMetrics) {
		int nrMetrics = costModel.nrMetrics;
		boolean[] consideredMetric = new boolean[nrMetrics];
		Arrays.fill(consideredMetric, false);
		int nrTrueValues = 0;
		while (nrTrueValues < nrEnabledMetrics) {
			int randomMetric = random.nextInt(nrMetrics);
			if (!consideredMetric[randomMetric]) {
				consideredMetric[randomMetric] = true;
				++nrTrueValues;
			}
		}
		return consideredMetric;
	}
	/**
	 * Generates test cases on which optimization algorithms are compared. Each test case
	 * consists of a query and a set of considered cost metrics.
	 * 
	 * @return A matrix of test cases containing a set of test cases for each query size.
	 */
	static TestCase[][] generateTestCases(JoinGraphType joinGraph, JoinType joinType, int nrEnabledMetrics) {
		TestCase[][] testcases = new TestCase[nrQuerySizes][nrTestCasesPerConf];
		for (int sizeCtr=0; sizeCtr<nrQuerySizes; ++sizeCtr) {
			int nrTables = nrQueryTables(sizeCtr);
			for (int testCaseCtr=0; testCaseCtr<nrTestCasesPerConf; ++testCaseCtr) {
				Query query = QueryFactory.produce(
						joinGraph, nrTables, maxTableCardinality, joinType);
				boolean[] consideredMetric = randomMetrics(nrEnabledMetrics);
				TestCase testcase = new TestCase(query, consideredMetric);
				testcases[sizeCtr][testCaseCtr] = testcase;
				System.out.println(testcase);
			}
		}
		return testcases;
	}
	/**
	 * Generates for each test case an approximated Pareto plan frontier by calling
	 * all benchmarked algorithms and taking the union of the plans they return. 
	 * 
	 * @param testcases	the test cases for which to find Pareto frontier
	 * @return			a matrix containing for each test case an approximate Pareto frontier
	 */
	static ParetoPlanSet[][] generateReferenceSolutions(TestCase[][] testcases) {
		// This variable is finally returned
		ParetoPlanSet[][] referenceSolutions = new ParetoPlanSet[nrQuerySizes][nrTestCasesPerConf];
		// Iterate over test case configurations
		for (int sizeCtr=0; sizeCtr<nrQuerySizes; ++sizeCtr) {
			// Iterate over test cases
			for (int testcaseCtr=0; testcaseCtr<nrTestCasesPerConf; ++testcaseCtr) {
				// Extract description of test case
				TestCase testcase = testcases[sizeCtr][testcaseCtr];
				Query query = testcase.query;
				boolean[] consideredMetric = testcase.consideredMetrics;
				// Generate reference Pareto plan set by accumulating result plans over all
				// algorithms when running them until the timeout.
				Statistics.disable();
				long tempTimeout = Constants.TIMEOUT_MILLIS;
				Constants.TIMEOUT_MILLIS = 300000;
				System.out.println(
						"Generating reference plans size " + sizeCtr + " test case " + testcaseCtr);
				Optimizer optimizer = new DPmoqo(1.01);
				List<Plan> referencePlans = optimizer.approximateParetoSet(
						query, consideredMetric, planSpace, costModel, null, 
						0, sizeCtr, testcaseCtr).plans;
				Constants.TIMEOUT_MILLIS = tempTimeout;
				TestUtil.validatePlans(referencePlans, planSpace, costModel, false);
				System.out.println("Generated " + referencePlans.size() + " reference plans");
				referenceSolutions[sizeCtr][testcaseCtr] = new ParetoPlanSet(referencePlans);
			}
		}
		return referenceSolutions;
	}
	/**
	 * Compare performance of algorithms on all test cases and store statistics.
	 * 
	 * @param testcases				test case queries to optimize
	 * @param referenceSolutions	approximation of Pareto frontier for test case queries
	 */
	static void compareAlgorithms(TestCase[][] testcases, ParetoPlanSet[][] referenceSolutions) {
		for (int optimizerCtr=0; optimizerCtr<nrOptimizers; ++optimizerCtr) {
			Optimizer optimizer = optimizers[optimizerCtr];
			// Warm up to achieve steady-state performance as recommended by Brent Boyer
			System.out.println("Starting code warmup");
			Statistics.disable();
			/*
			long warmupStart = System.currentTimeMillis();
			while (System.currentTimeMillis() - warmupStart < 10000) {
				int randomSize = random.nextInt(nrQuerySizes);
				int randomCase = random.nextInt(nrTestCasesPerConf);
				TestCase testcase = testcases[randomSize][randomCase];
				Query query = testcase.query;
				boolean[] consideredMetrics = testcase.consideredMetrics;
				optimizer.approximateParetoSet(query, consideredMetrics, 
						planSpace, costModel, null, 0, 0, 0);
			}
			// Garbage collection
			System.out.println("Starting garbage collection");
			for (int i=0; i<15; ++i) {
				System.gc();
			}
			*/
			// Iterate over test case configurations
			System.out.println("Starting benchmark");
			Statistics.enable();
			for (int sizeCtr=0; sizeCtr<nrQuerySizes; ++sizeCtr) {
				// Iterate over test cases
				for (int testcaseCtr=0; testcaseCtr<nrTestCasesPerConf; ++testcaseCtr) {
					System.out.println("Generating statistics size " + sizeCtr + 
							" test case " + testcaseCtr + " optimizer " + optimizerCtr);
					// Extract test case description and reference solution
					TestCase testcase = testcases[sizeCtr][testcaseCtr];
					Query query = testcase.query;
					boolean[] consideredMetric = testcase.consideredMetrics;
					ParetoPlanSet refPlanSet = referenceSolutions[sizeCtr][testcaseCtr];
					// Benchmark how well the algorithm approximates the reference plan set.
					optimizer.approximateParetoSet(query, consideredMetric, planSpace, 
							costModel, refPlanSet, optimizerCtr, sizeCtr, testcaseCtr);
				}
			}
		}
	}
	/**
	 * Runs a benchmark that compares the performance of algorithms for multi-objective
	 * query optimization on randomly generated queries of different size, potentially
	 * varying the join graph structure, the number of cost metrics, and the type of
	 * join predicate. Writes files to disc containing aggregate performance statistics.
	 * 
	 * @param args			(not used)
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// Iterate over type of join predicates (i.e., method by which selectivity is calculated)
		for (JoinType joinType : new JoinType[] {JoinType.MN}) {
			// Iterate over the number of plan cost metrics
			//for (int nrEnabledMetrics=2; nrEnabledMetrics<=3; ++nrEnabledMetrics) {
			for (int nrEnabledMetrics=3; nrEnabledMetrics<=3; ++nrEnabledMetrics) {
				// Iterate over join graph structure
				for (JoinGraphType joinGraph : new JoinGraphType[] {
						JoinGraphType.CHAIN, JoinGraphType.STAR, JoinGraphType.CYCLE}) {
					// Initialize component storing performance statistics
					Statistics.init(nrOptimizers, nrQuerySizes, 
							Constants.NR_TIME_PERIODS, nrTestCasesPerConf);
					// Generate test cases
					System.out.println("Benchmark for revision");
					System.out.println("Generating test cases");
					TestCase[][] testcases = generateTestCases(joinGraph, joinType, nrEnabledMetrics);
					// Generating reference solutions
					System.out.println("Generating reference solutions");
					ParetoPlanSet[][] referenceSolution = generateReferenceSolutions(testcases);
					// Solve test cases
					System.out.println("Comparing algorithms");
					compareAlgorithms(testcases, referenceSolution);
					// Output benchmark configuration
					System.out.println();
					System.out.println();
					System.out.println("SAFE_MODE: " + Constants.SAFE_MODE);
					System.out.println("Evaluated algorithms: ");
					System.out.println(Arrays.toString(optimizers));
					System.out.println("JoinGraph: " + joinGraph);
					System.out.println("JoinType: " + joinType);
					System.out.println("nrQuerySizes: " + nrQuerySizes);
					System.out.println("querySizeOffset: " + querySizeOffset);
					System.out.println("querySizeStep: " + querySizeStep);
					System.out.println("Smallest number query tables: " + nrQueryTables(0));
					System.out.println("Largest number query tables: " + nrQueryTables(nrQuerySizes-1));
					System.out.println("nrTestCasesPerConf: " + nrTestCasesPerConf);
					System.out.println("maxTableCardinality: " + maxTableCardinality);
					System.out.println("planSpace: " + planSpace);
					System.out.println("costModel: " + costModel);
					System.out.println("nrEnabledMetrics: " + nrEnabledMetrics);
					// *** Write results to file ***
					// define features to output
					String[] featureNames = new String[] {
							"Epsilon approximation after X-th time period",
					};
					int nrFeaturesToOutput = featureNames.length;
					// Iterate over all features that shall be written to disc
					for (int featureCtr=0; featureCtr<nrFeaturesToOutput; ++featureCtr) {
						String featureName = featureNames[featureCtr];
						// Iterate over all possible aggregation functions
						AggregateFunction function = AggregateFunction.MEDIAN;
						// Iterate over all query sizes
						for (int sizeCtr=0; sizeCtr<nrQuerySizes; ++sizeCtr) {
							String fileName = nrEnabledMetrics + "M" + nrQueryTables(sizeCtr) + "T" +
									joinGraph.name() + joinType.name() + ".txt";
							Statistics.writeToFile(featureName, function, sizeCtr, fileName);															
						}
					} // over features
				} // over join graph type
			} // over number of metrics
		} // over join types
	}

}
