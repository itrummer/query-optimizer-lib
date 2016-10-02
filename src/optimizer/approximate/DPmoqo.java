package optimizer.approximate;

import static common.Constants.*;

import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import benchmark.Statistics;
import cost.MultiCostModel;
import optimizer.Optimizer;
import plans.JoinOrderSpace;
import plans.ParetoPlanSet;
import plans.Plan;
import plans.JoinPlan;
import plans.ScanPlan;
import plans.spaces.PlanSpace;
import plans.operators.*;
import queries.Query;
import relations.Relation;
import relations.RelationFactory;
import util.ParetoUtil;
import util.PruningUtil;
import util.TestUtil;
 
/**
 * MOQO algorithm by Trummer and Koch (SIGMOD 2014) based on dynamic programming.
 * The algorithm formally guarantees to return an alpha-approximate Pareto frontier.
 * 
 * @author immanueltrummer
 *
 */
@SuppressWarnings("serial")
public class DPmoqo extends Optimizer {
	/**
	 * Global approximation factor. The cost of the generated query plans is formally
	 * guaranteed not to be higher than optimal by more than that factor for each plan
	 * cost metric.
	 */
	private final double globalAlpha;
	/**
	 * Join order space to be searched: either the optimizer considers only linear plan
	 * or it considers all possible join trees (bushy).
	 */
	private final JoinOrderSpace joinOrderSpace;
	
	public DPmoqo(double globalAlpha, JoinOrderSpace joinOrderSpace) {
		this.globalAlpha = globalAlpha;
		this.joinOrderSpace = joinOrderSpace;
	}
	
	/**
	 * Initializes global alpha and sets join order space to bushy as default.
	 * 
	 * @param globalAlpha	The cost of generated query plans is not higher than optimal by more than that.
	 */
	public DPmoqo(double globalAlpha) {
		this(globalAlpha, JoinOrderSpace.BUSHY);
	}
	
	// Count intermediate result creation
	void countResultCreation(int algIndex, int sizeIndex, int queryIndex) {
		String featureName = "#Created intermediate results";
		Statistics.addToLongFeature(featureName, algIndex, sizeIndex, 0, queryIndex, 1);
	}
	
	// create relation as join of two relations that are already in the relations list
	private Relation createRel(Query query, Map<BitSet, Relation> relations, 
			BitSet resultSet, int algIndex, int sizeIndex, int queryIndex) {
		// left join operand consists of only one table
		int firstTableIndex = resultSet.nextSetBit(0);
		BitSet leftSet = new BitSet();
		leftSet.set(firstTableIndex);
		// right join operand consists of remaining result tables
		BitSet rightSet = (BitSet)resultSet.clone();
		rightSet.clear(firstTableIndex);
		// look up corresponding relations - they must have been created before
		Relation leftRel = relations.get(leftSet);
		Relation rightRel = relations.get(rightSet);
		// form result relation as join between those two
		Relation resultRel = RelationFactory.createJoinRel(query, leftRel, rightRel);
		countResultCreation(algIndex, sizeIndex, queryIndex);
		return resultRel;
	}
	
	// Returns approximate Pareto plan set for given query with given approximation precision
	// and potentially considering a subset of cost metrics.
	@Override
	public ParetoPlanSet approximateParetoSet(Query query, boolean[] consideredMetrics,
			PlanSpace planSpace, MultiCostModel costModel, ParetoPlanSet refPlanSet, 
			int algIndex, int sizeIndex, int queryIndex) {
		// Register start time to check for timeouts
		long startMillis = System.currentTimeMillis();
		boolean timeout = false;
		// initialize variables
		int nrTables = query.nrTables;
		// Calculate local alpha from global alpha
		double localAlpha = Math.pow(globalAlpha, 1.0/nrTables);
		// initialize index of full query table set
		BitSet allTablesSet = new BitSet();
		for (int tableIndex=0; tableIndex<nrTables; ++tableIndex) {
			allTablesSet.set(tableIndex);
		}
		// maps table sets to corresponding relations
		Map<BitSet, Relation> relations = new HashMap<BitSet, Relation>();
		// treat single table relations
		for (int tableIndex=0; tableIndex<nrTables; ++tableIndex) {
			// create relation
			Relation rel = RelationFactory.createSingleTableRel(query, tableIndex);
			countResultCreation(algIndex, sizeIndex, queryIndex);
			// iterate over scan operators
			for (ScanOperator scanOp : planSpace.scanOperatorsShuffled(rel)) {
				Plan scanPlan = new ScanPlan(rel.cardinality, rel.pages, tableIndex, scanOp);
				costModel.updateRoot(scanPlan);
				PruningUtil.prune(query, rel, scanPlan, localAlpha, consideredMetrics, false);
			}
			// create relation index
			BitSet tableSet = new BitSet();
			tableSet.set(tableIndex);
			// insert relation
			relations.put(tableSet, rel);
		}
		// treat larger table sets in ascending order of cardinality
		for (int k=2; k<=nrTables; ++k) {
			BitSetIterator resultIter = new BitSetIterator(allTablesSet, k);
			// for all table sets of cardinality k
			while (resultIter.hasNext()) {
				BitSet resultSet = resultIter.next();
				// create and insert result relation
				Relation resultRel = createRel(query, relations, 
						resultSet, algIndex, sizeIndex, queryIndex);
				relations.put(resultSet, resultRel);
				// If we consider only left-deep (linear) plans then the size of the left join
				// operand must be one less than the result set size since the right join operand
				// is a single table. No such restrictions apply for bushy plans.
				int smallestLeftCardinality = joinOrderSpace == JoinOrderSpace.LINEAR ? k-1 : 1;
				// iterate over cardinality of result subset that forms left operand for final join
				for (int kLeft=smallestLeftCardinality; kLeft<k; ++kLeft) {
					BitSetIterator leftIter = new BitSetIterator(resultSet, kLeft);
					// for all possible left operands with given cardinality
					while (leftIter.hasNext()) {
						BitSet leftSet = leftIter.next();
						// right operand is complement of left operand in result table set
						BitSet rightSet = (BitSet)resultSet.clone();
						rightSet.andNot(leftSet); 
						// get corresponding relations
						Relation leftRel = relations.get(leftSet);
						Relation rightRel = relations.get(rightSet);
						// iterate over (near-)Pareto-optimal plans for left and right relation
						for (Plan leftPlan : leftRel.ParetoPlans) {
							for (Plan rightPlan : rightRel.ParetoPlans) {
								// iterate over all possible join methods
								for (JoinOperator joinOperator : planSpace.joinOperatorsShuffled(
										leftPlan, rightPlan)) {
									Plan newPlan = new JoinPlan(resultRel.cardinality, 
											resultRel.pages, leftPlan, rightPlan, joinOperator);
									costModel.updateRoot(newPlan);
									PruningUtil.prune(query, resultRel, newPlan, 
											localAlpha, consideredMetrics, false);
								} // over join operators
							} // over right plan
						} // over left plan
						// Check for timeouts
						if (System.currentTimeMillis() - startMillis > TIMEOUT_MILLIS) {
							timeout = true;
							break;
						}
					} // over left table set
					if (timeout) {
						break;
					}
				} // over left table set cardinality
				if (timeout) {
					break;
				}
			} // over result table set
			if (timeout) {
				break;
			}
		} // over result table set cardinality
		// return Pareto plans for joining all tables
		Relation resultRel = relations.get(allTablesSet);
		List<Plan> resultPlans = resultRel != null ? 
				resultRel.ParetoPlans : new LinkedList<Plan>();
		// Update statistics
		{
			String featureName = "#Pareto plans";
			long nrParetoPlans = resultPlans.size();
			Statistics.addToDoubleFeature(featureName, 
					algIndex, sizeIndex, 0, queryIndex, nrParetoPlans);
		}
		// calculate current time period that we are in
		long timePeriodMillis = TIMEOUT_MILLIS/NR_TIME_PERIODS;
		long millisPassed = System.currentTimeMillis() - startMillis;
		int curTimePeriod = (int)(millisPassed/timePeriodMillis);
		// update Pareto epsilon statistic (for MOQO benchmarks)
		if (refPlanSet != null){
			double curEpsilon = ParetoUtil.epsilonError(
					resultPlans, refPlanSet.plans, consideredMetrics);
			for (int periodCtr=0; periodCtr<NR_TIME_PERIODS; ++periodCtr) {
				String featureName = "Epsilon approximation after X-th time period";
				double epsilon = periodCtr >= curTimePeriod ? curEpsilon : Double.POSITIVE_INFINITY;
				Statistics.addToDoubleFeature(featureName, 
						algIndex, sizeIndex, periodCtr, queryIndex, epsilon);
			}
		}
		// update cost gap statistic (for comparison against ILP)
		{
			String featureName = "cost gap after X-th time period";
			for (int periodCtr=0; periodCtr<NR_TIME_PERIODS; ++periodCtr) {
				double gap = periodCtr >= curTimePeriod ? 1 : Double.POSITIVE_INFINITY;
				Statistics.addToDoubleFeature(featureName, 
						algIndex, sizeIndex, periodCtr, queryIndex, gap);	
			}
		}
		if (SAFE_MODE) {
			TestUtil.validatePlans(resultPlans, planSpace, costModel, false);
		}
		return new ParetoPlanSet(resultPlans);
	}
	
	@Override
	public String toString() {
		return "DP(alpha=" + globalAlpha + ")";
	}

}
