package optimizer.parallelized.partitioning;

import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import cost.MultiCostModel;
import optimizer.parallelized.Master;
import plans.JoinOrderSpace;
import plans.Plan;
import plans.spaces.PlanSpace;
import queries.Query;
import util.PruningUtil;

/**
 * The master assigns plan space partitions to slaves that find the best plan within each partition.
 * Afterwards, the master collects the resulting plans and returns the best one of those.
 * 
 * @author immanueltrummer
 *
 */
public class PartitioningMaster extends Master {
	/**
	 * Used to verify the class version.
	 */
	private static final long serialVersionUID = 1L;
	
	public PartitioningMaster(JoinOrderSpace joinOrderSpace, double alpha) {
		super(joinOrderSpace, alpha);
	}
	/**
	 * Creates a Spark context, partitions the query plan search space and
	 * generates tasks for finding the best plan in each plan space partition.
	 * Those tasks are distributed over the worker nodes and the best plans
	 * in each partition are collected from them as results. Then the master
	 * determines the generally best plan by comparing the best plans in 
	 * different plan space partitions. The master measure multiple 
	 * performance statistics that are stored in the corresponding fields.
	 */
	@Override
	protected void solveTestcase(Query query, boolean[] consideredMetrics, PlanSpace planSpace, 
			MultiCostModel costModel, double alpha, int degreeOfParallelism, 
			JavaSparkContext sparkContext, long timeoutMillis) {
		System.out.println("Solving new test case with partitioning and DOP=" + degreeOfParallelism);
		// Initialize performance statistics
		long startMillis = System.currentTimeMillis();
		lastRunBytesSent = 0;
		lastRunMainMemory = 0;
		lastRunMaxSlaveMillis = 0;
		lastRunMinSlaveMillis = Long.MAX_VALUE;
		lastRunTimeouts = false;
		lastRunMemoryouts = false;
		lastRunNrParetoPlans = 0;
	    // Create local collection containing task descriptions for the workers
	    List<PartitioningSlaveTask> tasksLocal = new LinkedList<PartitioningSlaveTask>();
	    for (int partitionID = 0; partitionID<degreeOfParallelism; ++partitionID) {
	    	PartitioningSlaveTask slaveTask = new PartitioningSlaveTask(query, joinOrderSpace, 
	    			planSpace, costModel, consideredMetrics, alpha, partitionID, degreeOfParallelism,
	    			timeoutMillis);
	    	tasksLocal.add(slaveTask);
	    }
	    System.out.println("Generated tasks");
	    lastRunBytesSent += sizeof(tasksLocal);
	    System.out.println("Counted byte size of tasks");
	    // Parallelize collection and map each task to the best query plan in the corresponding partition
	    JavaRDD<PartitioningSlaveTask> parallelizedTasks = 
	    		sparkContext.parallelize(tasksLocal, degreeOfParallelism);
	    System.out.println("Parallelized tasks");
	    @SuppressWarnings("serial")
		JavaRDD<PartitioningSlaveResult> results = parallelizedTasks.map(
				new Function<PartitioningSlaveTask, PartitioningSlaveResult>() {
					public PartitioningSlaveResult call(PartitioningSlaveTask t) {
						return PartitioningSlave.optimize(t);}
	    });
	    System.out.println("Found best plans in each plan space partition");
	    // Collect result plans from all workers
	    List<PartitioningSlaveResult> resultsLocal = results.collect();
	    System.out.println("Collected best plans in each plan space partition");
	    // Determine the overall best plan by comparing best plans from different plan space partitions
	    lastRunBestPlanList = new LinkedList<Plan>();
	    for (PartitioningSlaveResult result : resultsLocal) {
	    	// Update statistics based on results
	    	lastRunMainMemory = Math.max(lastRunMainMemory, result.mainMemoryConsumption);
	    	lastRunMaxSlaveMillis = Math.max(lastRunMaxSlaveMillis, result.slaveTaskMillis);
	    	lastRunMinSlaveMillis = Math.min(lastRunMinSlaveMillis, result.slaveTaskMillis);
	    	lastRunTimeouts = lastRunTimeouts || result.timeout;
	    	lastRunMemoryouts = lastRunMemoryouts || result.memoryOut;
	    	lastRunNrParetoPlans += result.paretoPlans == null ? 0 : result.paretoPlans.size();
	    	// Output statistics of current slave
	    	System.out.println("Slave memory: " + result.mainMemoryConsumption);
	    	System.out.println("Slave millis: " + result.mainMemoryConsumption);
	    	System.out.println("Slave timeout: " + result.timeout);
	    	System.out.println("Slave memory out: " + result.memoryOut);
	    	System.out.println("Checking for errors");
	    	String errors = result.errors == null ? "none" : result.errors;
	    	System.out.println("Any errors: " + errors);
	    	// check for timeout indicated by a null pointer
	    	if (result.paretoPlans == null) {
	    		System.out.println("Timeout occurred - collecting statistics");
	    		System.out.println("ATTENTION: we do not integrate statistics from other slaves!");
	    		lastRunMainMemory = Math.max(lastRunMainMemory, result.mainMemoryConsumption);
	    		lastRunMillis = System.currentTimeMillis() - startMillis;
	    		lastRunBytesSent += sizeof(resultsLocal);
	    		lastRunBestPlanList = null;
	    		System.out.println("Finished solving test case");
	    		return;
	    	}
	    	for (Plan partitionParetoPlan : result.paretoPlans) {
	    		System.out.println("Optimal plan in partition: " + partitionParetoPlan);
		    	PruningUtil.pruneCostBased(lastRunBestPlanList, partitionParetoPlan, consideredMetrics);	    		
	    	}
	    }
	    // Update performance statistics
	    System.out.println("Collecting statistics");
	    lastRunMillis = System.currentTimeMillis() - startMillis;
	    lastRunBytesSent += sizeof(resultsLocal);
	    System.out.println("Finished solving test case");
	}
	/**
	 * Class identifier for the master.
	 * 
	 * @return	short String identifier for the master
	 */
	@Override
	public String masterID() {
		return "PartitioningMaster(" + joinOrderSpace.toString() + ")"; 
	}
}
