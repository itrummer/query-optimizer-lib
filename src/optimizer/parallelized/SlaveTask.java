package optimizer.parallelized;

import java.io.Serializable;

import cost.CostModel;
import plans.JoinOrderSpace;
import plans.spaces.PlanSpace;
import queries.Query;

/**
 * Describes the optimization task to perform by a worker node.
 * 
 * @author immanueltrummer
 *
 */
public class SlaveTask implements Serializable {
	/**
	 * Used to verify class version.
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * the query to optimize
	 */
	public final Query query;
	 /**
	  * whether linear or bushy query plans are considered
	  */
	public final JoinOrderSpace joinOrderSpace;
	/**
	 * Determines the set of admissible scan and join operators 
	 */
	public final PlanSpace planSpace;
	/**
	 * Estimates plan execution cost according to multiple cost metrics
	 */
	public final CostModel costModel;
	/**
	 * Boolean flags indicating which plan cost metrics are considered
	 */
	public final boolean[] consideredMetrics;
	/**
	 * target approximation factor
	 */
	public final double alpha;
	/**
	 * Number of milliseconds until timeout
	 */
	public final long timeoutMillis;
	
	public SlaveTask(Query query, JoinOrderSpace joinOrderSpace, PlanSpace planSpace, 
	CostModel costModel, boolean[] consideredMetrics, double alpha, long timeoutMillis) {
		this.query = query;
		this.joinOrderSpace = joinOrderSpace;
		this.planSpace = planSpace;
		this.costModel = costModel;
		this.consideredMetrics = consideredMetrics;
		this.alpha = alpha;
		this.timeoutMillis = timeoutMillis;
	}
}
