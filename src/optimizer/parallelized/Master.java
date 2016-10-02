package optimizer.parallelized;

import static common.Constants.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;

import benchmark.Statistics;
import common.Constants;
import cost.MultiCostModel;
import optimizer.ParallelOptimizer;
import plans.JoinOrderSpace;
import plans.ParetoPlanSet;
import plans.Plan;
import plans.spaces.PlanSpace;
import queries.Query;

/**
 * Superclass of several optimization algorithms that can be parallelized on a large cluster.
 * 
 * @author immanueltrummer
 *
 */
public abstract class Master extends ParallelOptimizer {
	/**
	 * Used to verify the class version.
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * Whether only left deep or also bushy query plans should be considered by the optimizer.
	 */
	protected final JoinOrderSpace joinOrderSpace;
	/**
	 * Approximation factor to use during pruning.
	 */
	protected final double alpha;
	/**
	 * The number of milliseconds consumed for the last optimizer invocation.
	 */
	protected long lastRunMillis;
	/**
	 * The number of bytes sent over the network during the last optimizer invocation.
	 */
	protected long lastRunBytesSent;
	/**
	 * The maximal amount of main memory consumed by any node during the last optimizer invocation.
	 */
	protected long lastRunMainMemory;
	/**
	 * Maximal number of milliseconds used for processing any of the slave tasks during last invocation.
	 */
	protected long lastRunMaxSlaveMillis;
	/**
	 * Minimal number of milliseconds used for processing any of the slave tasks during last invocation.
	 */
	protected long lastRunMinSlaveMillis;
	/**
	 * Whether at least one of the slaves had a timeout during the last invocation.
	 */
	protected boolean lastRunTimeouts;
	/**
	 * Whether at least one of the slaves had a memoryout during the last invocation.
	 */
	protected boolean lastRunMemoryouts;
	/**
	 * The number of Pareto plans collected from the workers in the last run.
	 */
	protected int lastRunNrParetoPlans;

	/**
	 * Contains the best (=Pareto-optimal) plans generated in the last run.
	 */
	protected List<Plan> lastRunBestPlanList;
	
	public Master(JoinOrderSpace joinOrderSpace, double alpha) {
		this.joinOrderSpace = joinOrderSpace;
		this.alpha = alpha;
	}
	/**
	 * Calculates the size of an object in bytes by serializing it.
	 * 
	 * @param obj			the object whose size we want to calculate
	 * @return				long value representing the number of bytes consumed by the object
	 * @throws IOException
	 */
	public static long sizeof(Object obj) {
		try {
			ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteOutputStream);
			objectOutputStream.writeObject(obj);
			objectOutputStream.flush();
			objectOutputStream.close();
			return byteOutputStream.toByteArray().length;			
		} catch (Exception e) {
			assert(false);
			return 0;
		}
	}
	/**
	 * Solve the given test case by a sub-class specific parallelized optimization algorithm
	 * and store performance statistics about the run.
	 * 
	 * @param query					the query being optimized
	 * @param consideredMetrics		Boolean flags indicating whether specific cost metrics are considered
	 * @param planSpace				determines the set of applicable scan and join operators
	 * @param costModel				estimates the execution cost of query plans according to multiple metrics
	 * @param alpha					approximation factor to use during pruning
	 * @param degreeOfParallelism	the degree of parallelism to use for the optimization 
	 * @param sparkContext			Java spark context
	 * @param timeoutMillis			number of milliseconds after which a timeout is registered
	 */
	protected abstract void solveTestcase(Query query, boolean[] consideredMetrics, 
			PlanSpace planSpace, MultiCostModel costModel, double alpha,
			int degreeOfParallelism, JavaSparkContext sparkContext, long timeoutMillis);
	/**
	 * Optimizes one query multiple times with different degrees of parallelism
	 * and logs the performance results.
	 */
	@Override
	public ParetoPlanSet approximateParetoSet(Query query,
			boolean[] consideredMetrics, PlanSpace planSpace,
			MultiCostModel costModel, ParetoPlanSet refPlanSet, int algIndex,
			int sizeIndex, int queryIndex, JavaSparkContext sparkContext) {
		// So many levels of degree of parallelism are tried per query 
		int nrDegrees = DEGREES_OF_PARALLELISM.length;
		// Iterate over the degree of parallelism
		for (int parallelismIndex=0; parallelismIndex<nrDegrees; ++parallelismIndex) {
			// Get degree of parallelism for this iteration
			System.out.println("Retrieving degree of parallelism for index " + parallelismIndex);
			int degreeOfParallelism = DEGREES_OF_PARALLELISM[parallelismIndex];
			System.out.println("Degree of parallelism is " + degreeOfParallelism);
			// Solve the given test case
			System.out.println("About to solve test case");
			solveTestcase(query, consideredMetrics, planSpace, costModel, alpha,
					degreeOfParallelism, sparkContext, Constants.TIMEOUT_MILLIS);
			System.out.println("Test case solved");
			// Log the obtained performance statistics
			Statistics.addToLongFeature("millis", algIndex, sizeIndex, 
					parallelismIndex, queryIndex, lastRunMillis);
			Statistics.addToLongFeature("network", algIndex, sizeIndex, 
					parallelismIndex, queryIndex, lastRunBytesSent);
			Statistics.addToLongFeature("mainMemory", algIndex, sizeIndex, 
					parallelismIndex, queryIndex, lastRunMainMemory);
			Statistics.addToLongFeature("maxSlaveMillis", algIndex, sizeIndex, 
					parallelismIndex, queryIndex, lastRunMaxSlaveMillis);
			Statistics.addToLongFeature("minSlaveMillis", algIndex, sizeIndex, 
					parallelismIndex, queryIndex, lastRunMaxSlaveMillis);
			Statistics.addToLongFeature("timeouts", algIndex, sizeIndex, 
					parallelismIndex, queryIndex, lastRunTimeouts ? 1 : 0);
			Statistics.addToLongFeature("memoryouts", algIndex, sizeIndex, 
					parallelismIndex, queryIndex, lastRunMemoryouts ? 1 : 0);
			Statistics.addToLongFeature("nrParetoPlans", algIndex, sizeIndex, 
					parallelismIndex, queryIndex, lastRunNrParetoPlans);
			// Output result
			System.out.println("*** RESULTS FOR DOP " + degreeOfParallelism + " *** ");
			System.out.println("Master: " + this.masterID());
			System.out.println("algIndex: " + algIndex);
			System.out.println("sizeIndex: " + sizeIndex);
			System.out.println("queryIndex: " + queryIndex);
			System.out.println("millis: " + lastRunMillis);
			System.out.println("network: " + lastRunBytesSent);
			System.out.println("memory: " + lastRunMainMemory);
			System.out.println("maxSlaveMillis: " + lastRunMaxSlaveMillis);
			System.out.println("minSlaveMillis: " + lastRunMinSlaveMillis);
			System.out.println("timeouts: " + lastRunTimeouts);
			System.out.println("memoryouts: " + lastRunMemoryouts);
			System.out.println("nrParetoPlans: " + lastRunNrParetoPlans);
			System.out.println("Best plans:");
			if (lastRunBestPlanList != null) {
				for (Plan plan : lastRunBestPlanList) {
					System.out.println("Next plan");
					System.out.println(plan.toString());
				}				
			} else {
				System.out.println("No complete plans generated");
			}
		}
		// Parallelized optimizers are not compared in terms of plan quality hence we return null
		return null;
	}
	/**
	 * Class identifier for the master.
	 * 
	 * @return	short String identifier for the master
	 */
	public abstract String masterID();
}
