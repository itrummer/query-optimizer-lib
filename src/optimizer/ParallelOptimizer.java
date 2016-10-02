package optimizer;

import java.io.Serializable;

import org.apache.spark.api.java.JavaSparkContext;

import cost.MultiCostModel;
import plans.ParetoPlanSet;
import plans.spaces.PlanSpace;
import queries.Query;

public abstract class ParallelOptimizer implements Serializable {
	/**
	 * Used to verify class version.
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Approximate Pareto plan set for query and store corresponding statistics
	 * 
	 * @param query				the query being optimized
	 * @param consideredMetrics	Boolean flags indicating for each cost metric whether we consider it
	 * @param planSpace			the plan space determines the set of admissible scan and join operators
	 * @param costModel			cost model estimating plan cost according to different metrics
	 * @param refPlanSet		set of Pareto-optimal reference plans against which the optimizers compare
	 * @param algIndex			the algorithm index under which statistics should be stored
	 * @param sizeIndex			the query size index under which statistics should be stored
	 * @param queryIndex		the test case index under which statistics should be stored
	 * @param sparkContext		Spark context to use for parallelization
	 * @return					the set of Pareto-optimal query plans generated in this optimizer run
	 */
	public abstract ParetoPlanSet approximateParetoSet(Query query, boolean[] consideredMetrics, 
			PlanSpace planSpace, MultiCostModel costModel, ParetoPlanSet refPlanSet,
			int algIndex, int sizeIndex, int queryIndex, JavaSparkContext sparkContext);
}
