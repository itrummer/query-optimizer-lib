package benchmark;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import common.Constants;
import cost.MultiCostModel;
import cost.SingleCostModel;
import cost.local.BufferCostModel;
import cost.local.TimeCostModel;
import optimizer.ParallelOptimizer;
import optimizer.parallelized.partitioning.PartitioningMaster;
import plans.JoinOrderSpace;
import plans.spaces.LocalPlanSpace;
import plans.spaces.LocalSpaceVariant;
import plans.spaces.PlanSpace;
import queries.JoinGraphType;
import queries.JoinType;
import queries.Query;
import queries.QueryFactory;

/**
 * Benchmark for comparing different parallelized optimization algorithms.
 * 
 * @author immanueltrummer
 *
 */
public class RunParallelizationBenchmark {
	/**
	 * Outputs performance statistics about one specific configuration (join graph structure
	 * and join type).
	 * 
	 * @param joinGraph					the structure of the join graph
	 * @param joinType					the type of the joins (determines the selectivity of join predicates)
	 * @param nrQuerySizes				the number of considered query sizes (i.e., number of tables)
	 * @param querySizes				associates each query size index to the number of joined tables
	 * @param folder					directory into which to put result files
	 * @throws FileNotFoundException
	 */
	static void writeStatistics(JoinGraphType joinGraph, JoinType joinType, int nrQuerySizes, 
			int[] querySizes, String folder) throws FileNotFoundException {
		// Select features for output and aggregation functions
		String[] featureNames = new String[] {"millis", "network", "mainMemory", 
				"maxSlaveMillis", "minSlaveMillis", "timeouts", "memoryouts",
				"nrParetoPlans"};
		AggregateFunction[] aggregateFunctions = new AggregateFunction[] {
				AggregateFunction.MEDIAN, AggregateFunction.MEDIAN, AggregateFunction.MEDIAN,
				AggregateFunction.MEDIAN, AggregateFunction.MEDIAN, 
				AggregateFunction.MEAN, AggregateFunction.MEAN,
				AggregateFunction.MEDIAN
		};
		int nrFeaturesToOutput = featureNames.length;
		// Write results into file
		for (int querySizeIndex=0; querySizeIndex<nrQuerySizes; ++querySizeIndex) {
			// The configuration name captures join graph, join type, and query size
			int querySize = querySizes[querySizeIndex];
			String configurationName = joinGraph.name() + "_" + joinType.name() + "_" + "S" + querySize;
			// Iterate over all features that shall be written to disc
			for (int featureCtr=0; featureCtr<nrFeaturesToOutput; ++featureCtr) {
				String featureName = featureNames[featureCtr];
				AggregateFunction function = aggregateFunctions[featureCtr];
				String fileName = featureName + "_" + function.name() + "_" + configurationName;
				System.out.println("START_FILE_OUTPUT " + fileName);
				// (featureName, function, querySizeIndex, standardWriter);
				PrintWriter standardWriter = new PrintWriter(System.out);
				Statistics.write(featureName, function, querySizeIndex, standardWriter);
				//Statistics.writeToFile(featureName, function, querySize, folder + "/" + fileName);
				System.out.println("END_FILE_OUTPUT");
			}
		}
	}
	/**
	 * Compares parallelized optimization algorithms for different query sizes,
	 * join graph structures, and join types and outputs the results.
	 * 
	 * @param args	number of test cases, 
	 * 				number of join tables (separated by semicolon),
	 * 				degrees of parallelism to try (separated by semicolon),
	 * 				timeout in seconds
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException {
		System.out.println("Starting parallel MOQO benchmark");
		// Verify number of command line arguments
		if (args.length != 4) {
			System.out.println("You need to specify four command line arguments: ");
			System.out.println("- the number of test cases per configuration");
			System.out.println("- the numbers of joined tables to try with (separated by semicolon)");
			System.out.println("- the degrees of parallelism to try with (separated by semicolon)");
			System.out.println("- the timeout per query in seconds");
			return;
		}
		// Extract command line arguments
		int nrTestcasesPerConfiguration = Integer.parseInt(args[0]);
		// Extract numbers of joined tables to consider
		String[] querySizesStrings = args[1].split(";");
		int nrQuerySizes = querySizesStrings.length;
		int[] QUERY_SIZES = new int[nrQuerySizes];
		for (int querySizeCtr=0; querySizeCtr<nrQuerySizes; ++querySizeCtr) {
			QUERY_SIZES[querySizeCtr] = Integer.parseInt(querySizesStrings[querySizeCtr]);
		}
		// Extract degrees of parallelism to try
		String[] dopStrings = args[2].split(";");
		int nrDegreesOfParallelism = dopStrings.length;
		Constants.DEGREES_OF_PARALLELISM = new int[nrDegreesOfParallelism];
		for (int dopCtr=0; dopCtr<nrDegreesOfParallelism; ++dopCtr) {
			Constants.DEGREES_OF_PARALLELISM[dopCtr] = Integer.parseInt(dopStrings[dopCtr]);
		}
		// Extract timeout
		Constants.TIMEOUT_MILLIS = 1000 * Integer.parseInt(args[3]);
		// Generate compared optimization algorithms
		ParallelOptimizer[] optimizers = new ParallelOptimizer[] {
			//new PartitioningMaster(JoinOrderSpace.LINEAR, 10),
			new PartitioningMaster(JoinOrderSpace.BUSHY, 10),
		};
		int nrOptimizers = optimizers.length;
		// Set output folder
		String folder = "linear";
		// Choose plan space and cost model
		/*
		PlanSpace planSpace = new LocalPlanSpace(LocalSpaceVariant.SOQO);
		Constants.NR_COST_METRICS = 1;
		MultiCostModel costModel = new MultiCostModel(Arrays.asList(
				new SingleCostModel[] {new TimeCostModel(0)}));
		boolean[] consideredMetrics = new boolean[] {true};
		*/
		PlanSpace planSpace = new LocalPlanSpace(LocalSpaceVariant.MOQO);
		MultiCostModel costModel = new MultiCostModel(Arrays.asList(
				new SingleCostModel[] {new TimeCostModel(0), new BufferCostModel(1)}));
		boolean[] consideredMetrics = new boolean[] {true, true};
		// Initialize statistics
		Statistics.init(nrOptimizers, nrQuerySizes, nrDegreesOfParallelism, nrTestcasesPerConfiguration);
		// Generate Spark context
		System.out.println("About to generate spark context");
		JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf());
		System.out.println("Generated spark context");
		// Iterate over join graph structures
		for (JoinGraphType joinGraph : new JoinGraphType[] {
				//JoinGraphType.CHAIN, JoinGraphType.STAR, JoinGraphType.CYCLE
				JoinGraphType.STAR
		}) {
			// Iterate over join types
			for (JoinType joinType : new JoinType[] {JoinType.MN}) {
				// Iterate over the number of query tables
				for (int querySizeIndex=0; querySizeIndex<nrQuerySizes; ++querySizeIndex) {
					int nrTables = QUERY_SIZES[querySizeIndex];
					// Iterate over test cases
					for (int testcaseCtr=0; testcaseCtr<nrTestcasesPerConfiguration; ++testcaseCtr) {
						// Generate test case
						Query query = QueryFactory.produceSteinbrunn(joinGraph, nrTables, joinType);
						// Solve test case by different optimizers
						for (int optimizerIndex=0; optimizerIndex<nrOptimizers; ++optimizerIndex) {
							ParallelOptimizer optimizer = optimizers[optimizerIndex];
							optimizer.approximateParetoSet(query, consideredMetrics, planSpace, 
									costModel, null, optimizerIndex, querySizeIndex, testcaseCtr,
									sparkContext);
						} 	// over optimizers
					}	// over test cases
				}	// over query sizes
				// Write performance statistics to files
				writeStatistics(joinGraph, joinType, nrQuerySizes, QUERY_SIZES, folder);
				System.out.println("Statistics written for " + joinGraph + "; " + joinType);
			}	// over join types
		}	// over join graph structure
		// close Spark context
		sparkContext.close();
	}
}
