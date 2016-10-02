package queries;

import cost.CostModel;
import plans.Plan;
import plans.ScanPlan;
import plans.operators.ScanOperator;
import plans.spaces.PlanSpace;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Represents a join query where the goal is to join all tables while applying join predicates
 * as early as possible. A query is characterized by the table cardinalities, the join graph
 * structure, and by the selectivity values of the predicates.
 * 
 * @author immanueltrummer
 *
 */
public class Query implements Serializable {
	/**
	 * Used to verify the class version.
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * The number of tables that need to be joined.
	 */
	public final int nrTables;
	/**
	 * The cardinality (number of rows) for each base table.
	 */
	public final double[] tableCardinalities;
	/**
	 * The selectivity between table pairs, if no join predicate is defined between
	 * two specific tables then the selectivity value must be one. Note that the
	 * selectivity matrix must be symmetric.
	 */
	public final double[][] selectivities;
	
	public Query(int nrTables, double[] tableCardinalities, double[][] selectivities) {
		assert(tableCardinalities.length == nrTables);
		assert(selectivities.length == nrTables);
		assert(selectivities[0].length == nrTables);
		this.nrTables = nrTables;
		this.tableCardinalities = tableCardinalities;
		this.selectivities = selectivities;
	}
	@Override
	public String toString() {
		String output = "Cardinalities:";
		for (int i=0; i<tableCardinalities.length; ++i) {
			output += " " + tableCardinalities[i];
		}
		output += System.lineSeparator() + "Selectivities:" + System.lineSeparator();
		for (int i=0; i<selectivities.length; ++i) {
			output += " " + Arrays.toString(selectivities[i]) + System.lineSeparator();
		}
		return output;
	}
	/**
	 * Returns the list of scan plans for all base tables in the query, using the default
	 * scan operator in the given plan space and using the cost model to assign scan plans
	 * to cost values.
	 * 
	 * @param planSpace		we use the default scan operator specified in that plan space
	 * @param costModel		we apply that cost model to calculate the cost for each scan plan
	 * @return				a list of scan plans covering all tables of this query
	 */
	public List<Plan> allScanPlans(PlanSpace planSpace, CostModel costModel) {
		List<Plan> allScanPlans = new LinkedList<Plan>();
		ScanOperator scanOperator = planSpace.defaultScanOperator;
		for (int tableIndex=0; tableIndex<nrTables; ++tableIndex) {
			Plan scanPlan = new ScanPlan(this, tableIndex, scanOperator);
			costModel.updateRoot(scanPlan);
			allScanPlans.add(scanPlan);
		}
		return allScanPlans;
	}
}
