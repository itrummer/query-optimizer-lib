package cost;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import plans.Plan;

/**
 * Aggregates multiple cost metrics in one cost model. This allows to conveniently calculate
 * the cost according to all cost metrics by just one cost model invocation.
 * 
 * @author immanueltrummer
 *
 */
public class MultiCostModel extends CostModel {
	/**
	 * Used to verify the class version.
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * Counts the number of plan node evaluations.
	 */
	public long nrRootCostEvaluations = 0;
	/**
	 * Number of single cost metrics aggregated by this multi-cost model.
	 */
	public final int nrMetrics;
	/**
	 * List of single cost metrics aggregated by this multi-cost model.
	 */
	final List<SingleCostModel> models;

	public MultiCostModel(List<SingleCostModel> models) {
		this.nrMetrics = models.size();
		this.models = models;
		assert(nrMetrics>0);
		// Make sure that metric indices are consistent
		Set<Integer> metricIndices = new TreeSet<Integer>();
		for (SingleCostModel model : models) {
			int metricIndex = model.metricIndex;
			assert(metricIndex<nrMetrics);
			assert(!metricIndices.contains(metricIndex));
			metricIndices.add(metricIndex);
		}
	}

	@Override
	public void updateRoot(Plan plan) {
		// Invoke component metrics
		for (SingleCostModel model : models) {
			model.updateRoot(plan);
		}
		++nrRootCostEvaluations;
	}

	@Override
	public String toString() {
		return models.toString();
	}
}
