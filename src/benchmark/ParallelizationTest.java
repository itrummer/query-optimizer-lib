package benchmark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import common.Constants;
import optimizer.approximate.DPmoqo;
import optimizer.parallelized.partitioning.PartitioningSlave;
import optimizer.parallelized.partitioning.PartitioningSlaveResult;
import optimizer.parallelized.partitioning.PartitioningSlaveTask;
import plans.JoinOrderSpace;
import plans.ParetoPlanSet;
import plans.Plan;
import queries.JoinGraphType;
import queries.JoinType;
import queries.Query;
import queries.QueryFactory;
import util.PruningUtil;
import util.TestUtil;

import java.util.LinkedList;
import java.util.List;

/**
 * Test version of benchmark for parallel query optimization.
 * 
 * @author immanueltrummer
 *
 */
public class ParallelizationTest {

	public static void main(String[] args) {
		// TODO: this must iterate over different values of course
		final int degreeOfParallelism = 4;
		// Create Spark context
		//SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
	    //JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaSparkContext ctx = new JavaSparkContext("local", "Simple App");
	    // Randomly generate query
	    Query query = QueryFactory.produceSteinbrunn(JoinGraphType.CHAIN, 6, JoinType.MN);
	    // Create local collection containing task descriptions for the workers
	    List<PartitioningSlaveTask> tasksLocal = new LinkedList<PartitioningSlaveTask>();
	    for (int partitionID = 0; partitionID<degreeOfParallelism; ++partitionID) {
	    	PartitioningSlaveTask slaveTask = new PartitioningSlaveTask(query, JoinOrderSpace.BUSHY, 
	    			TestUtil.planSpace, TestUtil.timeCostModel, new boolean[] {true}, 1, partitionID, 
	    			degreeOfParallelism, Constants.TIMEOUT_MILLIS);
	    	tasksLocal.add(slaveTask);
	    }
	    // Parallelize collection and map each task to the best query plan in the corresponding partition
	    JavaRDD<PartitioningSlaveTask> tasks = ctx.parallelize(tasksLocal, degreeOfParallelism);
	    @SuppressWarnings("serial")
		JavaRDD<PartitioningSlaveResult> results = tasks.map(
				new Function<PartitioningSlaveTask, PartitioningSlaveResult>() {
					public PartitioningSlaveResult call(PartitioningSlaveTask t) { 
						return PartitioningSlave.optimize(t); }
	    });
	    List<PartitioningSlaveResult> resultsLocal = results.collect();
	    // Determine the overall best plan by comparing best plans from different plan space partitions
	    List<Plan> bestPlanList = new LinkedList<Plan>();
	    for (PartitioningSlaveResult result : resultsLocal) {
	    	for (Plan partitionParetoPlan : result.paretoPlans) {
		    	PruningUtil.pruneCostBased(bestPlanList, partitionParetoPlan, new boolean[] {true});	    		
	    	}
	    }
	    // Print out best plan
	    Plan bestPlanParallel = bestPlanList.iterator().next();
	    System.out.println("Cost of best plan found in parallel: " + bestPlanParallel.cost[0]);
	    ctx.close();
	    // Verify that best plan calculated in parallel has same cost as best plan found by local optimizer
	    Statistics.init(1, 1, 1, 1);
	    DPmoqo dpMoqo = new DPmoqo(1);
	    ParetoPlanSet bestPlansLocal = dpMoqo.approximateParetoSet(query, new boolean[] {true}, 
	    		TestUtil.planSpace, TestUtil.timeCostModel, null, 0, 0, 0);
	    Plan bestPlanLocal = bestPlansLocal.plans.iterator().next();
	    System.out.println("Cost of best plan found locally: " + bestPlanLocal.cost[0]);
	}

}
