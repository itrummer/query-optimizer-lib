package optimizer.randomized.soqo.genetic;

import cost.CostModel;
import plans.JoinPlan;
import plans.Plan;
import plans.ScanPlan;
import plans.operators.JoinOperator;
import plans.operators.ScanOperator;
import plans.spaces.PlanSpace;
import queries.Query;

import java.util.LinkedList;
import java.util.List;

import org.jgap.FitnessFunction;
import org.jgap.IChromosome;

/**
 * Calculates a fitness value based on the cost value in the first
 * cost metric realized by a query plan encoded as chromosome.
 * 
 * @author immanueltrummer
 * 
 */
public class PlanFitness extends FitnessFunction {
	/**
	 * Used to verify class version.
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * The query that is currently being optimized.
	 */
	final Query query;
	/**
	 * The number of tables in the query that is currently optimized.
	 */
	final int nrTables;
	/**
	 * Left-deep plans answering the current query must perform that many joins.
	 */
	final int nrJoins;
	/**
	 * We use default scan and join operators from that plan space to build the plan.
	 */
	final PlanSpace planSpace;
	/**
	 * Default scan operator to use.
	 */
	final ScanOperator scanOperator;
	/**
	 * Default join operator to use.
	 */
	final JoinOperator joinOperator;
	/**
	 * Cost model used to calculate the cost of query plans.
	 */
	final CostModel costModel;

	public PlanFitness(Query query, PlanSpace planSpace, CostModel costModel) {
		this.query = query;
		this.nrTables = query.nrTables;
		this.nrJoins = nrTables - 1;
		this.planSpace = planSpace;
		this.scanOperator = planSpace.defaultScanOperator;
		this.joinOperator = planSpace.defaultJoinOperator;
		this.costModel = costModel;
	}
	/**
	 * Extract the query plan that is encoded by the given chromosome and
	 * calculate its cost.
	 * 
	 * @param planChromosome	a chromosome encoding a left-deep query plan
	 * @return					the query plan represented by the chromosome
	 */
	public Plan extractPlan(IChromosome planChromosome) {
		// Prepare list of tables that have not been used as join operands yet
		List<Integer> remainingTables = new LinkedList<Integer>();
		for (int table=0; table<nrTables; ++table) {
			remainingTables.add(table);
		}
		// Generate the query plan encoded in the chromosome join by join
		int firstTableIndex = (int)planChromosome.getGene(0).getAllele();
		int firstTable = remainingTables.remove(firstTableIndex);
		Plan plan = new ScanPlan(query, firstTable, scanOperator);
		// Iterate over joins
		for (int joinCtr=0; joinCtr<nrJoins; ++joinCtr) {
			int nextTableIndex = (int)planChromosome.getGene(joinCtr+1).getAllele();
			int nextTable = remainingTables.remove(nextTableIndex);
			ScanPlan rightPlan = new ScanPlan(query, nextTable, scanOperator);
			plan = new JoinPlan(query, plan, rightPlan, joinOperator);
		}
		assert(remainingTables.isEmpty());
		// calculate cost of encoded plan
		costModel.updateAll(plan);
		return plan;
	}
	@Override
	protected double evaluate(IChromosome planChromosome) {
		// extract plan encoded by the chromosome
		Plan plan = extractPlan(planChromosome);
		double cost = plan.cost[0];
		// calculate fitness value based on cost value
		double fitness = 1.0 / cost;
		// return fitness value
		return fitness;
	}

}
