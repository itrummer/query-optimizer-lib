package optimizer.randomized.moqo;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import benchmark.Statistics;
import cost.MultiCostModel;
import optimizer.randomized.RandomizedOptimizer;
import plans.JoinPlan;
import plans.Plan;
import plans.ScanPlan;
import plans.operators.JoinOperator;
import plans.operators.ScanOperator;
import plans.spaces.PlanSpace;
import queries.Query;
import relations.Relation;
import relations.RelationFactory;
import util.LocalSearchUtil;
import util.ParetoUtil;
import util.PruningUtil;

/**
 * Novel randomized algorithm for MOQO. This algorithm combines local search, typically
 * used in randomized query optimization algorithms, with partial plan caching, typically
 * used within dynamic programming based exhaustive query optimization algorithms.
 * The algorithm separates join order optimization from operator selection and approximates
 * the Pareto frontier for a given join order by varying operator selections.
 * The coarsening factor for that approximation is selected based on a UCB policy:
 * we calculate an optimistic estimate for each factor by how much it will improve the
 * current Pareto frontier approximation.
 * 
 * @author immanueltrummer
 *
 */
public class X extends RandomizedOptimizer {
	/**
	 * Maps table index sets to corresponding relations. The relation objects store lists of
	 * Pareto-optimal plans generating that relation that are used to improve randomly sampled
	 * plans. Also, each relation contains statistics that guides the selection of the next
	 * relation to sample a partial plan for.
	 */
	Map<BitSet, Relation> relations = new HashMap<BitSet, Relation>();
	/**
	 * Indices of all query tables.
	 */
	BitSet allTableIndices;
	/**
	 * Different coarsening factors for approximating the Pareto frontier; the algorithm
	 * can choose between those different coarsening factors.
	 */
	//final double[] coarseningFactors = new double[] {2, 10, 100, 1000};
	//final double[] coarseningFactors = new double[] {2, 5, 10, 100};
	final double[] coarseningFactors = new double[] {25};
	/**
	 * Each approximation factor is associated with a weight representing computational overhead.
	 */
	//final double[] coarseningWeight = new double[] {8, 4, 2, 1};
	final double[] coarseningWeight = new double[] {1};
	/**
	 * The number of alternative coarsening factors to choose from.
	 */
	final int nrFactors = coarseningFactors.length;
	/**
	 * The accumulated rewards associated with each of the coarsening factors.
	 */
	double[] accumulatedRewards = new double[nrFactors];
	/**
	 * The number of times each coarsening factor was used.
	 */
	long[] nrPlayed = new long[nrFactors];
	/**
	 * An internal counter of the number of refinement steps since the coarsening
	 * factor statistics were last initialized.
	 */
	long nrRoundsPlayed = 0;
	/**
	 * Initialize statistics that are used to select coarsening factor.
	 */
	void initCoarseningStats() {
		for (int factorCtr=0; factorCtr<nrFactors; ++factorCtr) {
			accumulatedRewards[factorCtr] = 0;
			nrPlayed[factorCtr] = 0;
		}
		nrRoundsPlayed = 0;
	}
	/**
	 * 
	 */
	long nrRefinements = 0;
	/**
	 * Returns the index of the coarsening factor with maximal UCB value.
	 * Makes sure that each factor is played at least once.
	 * 
	 * @return	the index of the most interesting coarsening factor
	 */
	int maxUCBcoarsening() {
		double maxUCB = Double.NEGATIVE_INFINITY;
		int maxUCBindex = -1;
		for (int factorCtr=0; factorCtr<nrFactors; ++factorCtr) {
			if (nrPlayed[factorCtr] == 0) {
				return factorCtr;
			}
			double averageReward = accumulatedRewards[factorCtr] / nrPlayed[factorCtr];
			double confidenceWidth = Math.sqrt(2 * Math.log(nrRoundsPlayed)/nrPlayed[factorCtr]);
			double curUCB = averageReward + confidenceWidth;
			if (curUCB > maxUCB) {
				maxUCB = curUCB;
				maxUCBindex = factorCtr;
			}
		}
		return maxUCBindex;
	}
	/**
	 * Updates statistics about coarsening factors based on last selected index.
	 * 
	 * @param newPlans				new plans that were generated during selection
	 * @param selectedFactorIndex	the index of last selected coarsening factor
	 * @param consideredMetrics		Boolean flags indicating which cost metrics are considered
	 */
	void updateFactorStats(List<Plan> newPlans, int selectedFactorIndex, boolean[] consideredMetrics) {
		assert(selectedFactorIndex < nrFactors);
		++nrRoundsPlayed;
		if (nrRoundsPlayed > 300) {
			System.out.println("#Factor selections: " + Arrays.toString(nrPlayed));
			initCoarseningStats();
		} else {
			nrPlayed[selectedFactorIndex] += 1;
			// Reward is based on how difficult it would be to approximate the new
			// plans by the old plans.
			double epsilonDelta = ParetoUtil.epsilonError(
					currentApproximation, newPlans, consideredMetrics);
			double weight = coarseningWeight[selectedFactorIndex];
			double reward = 1 - Math.exp(-epsilonDelta/weight);
			accumulatedRewards[selectedFactorIndex] += reward;
		}
	}
	/**
	 * Clear relations cache and reset current plan which is generated randomly 
	 * at next occasion.
	 */
	@Override
	protected void init(Query query, boolean[] consideredMetrics,
			PlanSpace planSpace, MultiCostModel costModel) {
		nrRefinements = 0;
		// Clear current relations
		relations.clear();
		// Store indices of all query tables as set
		int nrTables = query.nrTables;
		allTableIndices = new BitSet();
		for (int tableIndex=0; tableIndex<nrTables; ++tableIndex) {
			allTableIndices.set(tableIndex);
		}
		// Initialize coarsening statistics
		initCoarseningStats();
		// 
	}
	/**
	 * Notifies statistics component that one new intermediate result was generated.
	 * 
	 * @param algIndex		algorithm index in current benchmark
	 * @param sizeIndex		size index of currently optimized query
	 * @param queryIndex	index of query within its query size group
	 */
	void countIntermediateResultCreation(int algIndex, int sizeIndex, int queryIndex) {
		String featureName = "#Created intermediate results";
		Statistics.addToLongFeature(featureName, algIndex, sizeIndex, 0, queryIndex, 1);
	}
	/**
	 * Retrieves a relation representing a single table from relations store or creates and inserts
	 * the relation if it does not yet exist.
	 * 
	 * @param query			the query being optimized
	 * @param tableIndex	index of the table whose relation we need
	 * @param algIndex		algorithm index, used to collect statistics
	 * @param sizeIndex		query size index, used to collect statistics
	 * @param queryIndex	index of query within its query size group
	 * @return				a relation that represents the table with the given index
	 */
	Relation getSingleTableRel(Query query, int tableIndex, 
			int algIndex, int sizeIndex, int queryIndex) {
		BitSet singleTableIndices = new BitSet();
		singleTableIndices.set(tableIndex);
		Relation rel = relations.get(singleTableIndices);
		if (rel == null) {
			rel = RelationFactory.createSingleTableRel(query, tableIndex);
			relations.put(singleTableIndices, rel);
			countIntermediateResultCreation(algIndex, sizeIndex, queryIndex);
		}
		return rel;
	}
	/**
	 * Retrieves a relation representing the join between multiple tables from relations store or
	 * creates and inserts the corresponding relation.
	 * 
	 * @param query			the query being optimized
	 * @param leftRel		first join operand
	 * @param rightRel		second join operand
	 * @param algIndex		algorithm index, used to collect statistics
	 * @param sizeIndex		query size index, used to collect statistics
	 * @param queryIndex	index of query within its query size group
	 * @return				a relation representing the join between the given operands
	 */
	Relation getJoinRel(Query query, Relation leftRel, 
			Relation rightRel, int algIndex, int sizeIndex, int queryIndex) {
		BitSet joinTableIndices = new BitSet();
		joinTableIndices.or(leftRel.tableSet);
		joinTableIndices.or(rightRel.tableSet);
		Relation rel = relations.get(joinTableIndices);
		if (rel == null) {
			rel = RelationFactory.createJoinRel(query, leftRel, rightRel);
			relations.put(joinTableIndices, rel);
			countIntermediateResultCreation(algIndex, sizeIndex, queryIndex);
		}
		return rel;
	}
	/**
	 * Generates and returns a random bushy plan that uses the standard scan and join operators.
	 * 
	 * @param query			the query being optimized
	 * @param planSpace		determines the applicable join operators
	 * @param costModel		used to calculate the cost of the new plan
	 * @param algIndex		algorithm index, used to collect statistics
	 * @param sizeIndex		query size index, used to collect statistics
	 * @return				a randomly generated plan joining the given tables and producing
	 * 						output that is acceptable as input for the parent node
	 */
	Plan randomJoinOrder(Query query, PlanSpace planSpace,  
			MultiCostModel costModel, int algIndex, int sizeIndex) {
		int nrTables = query.nrTables;
		// Iterate over table indices and create scan plans
		List<Plan> partialPlans = new LinkedList<Plan>();
		for (int tableIndex = 0; tableIndex<nrTables; ++tableIndex) {
			ScanOperator scanOperator = planSpace.defaultScanOperator;
			Plan scanPlan = new ScanPlan(query, tableIndex, scanOperator);
			costModel.updateRoot(scanPlan);
			partialPlans.add(scanPlan);
		}
		// Combine partial plans until we obtain a complete plan.
		while (partialPlans.size()>1) {
			Plan leftPlan = LocalSearchUtil.popRandomPlan(partialPlans);
			Plan rightPlan = LocalSearchUtil.popRandomPlan(partialPlans);
			JoinOperator joinOperator = planSpace.defaultJoinOperator;
			Plan joinPlan = new JoinPlan(query, leftPlan, rightPlan, joinOperator);
			costModel.updateRoot(joinPlan);
			partialPlans.add(joinPlan);
		}
		return partialPlans.get(0);
	}
	/**
	 * Extracts non-dominated sub-plans and stores them in the plan cache. This method traverses
	 * the plan tree and stores Pareto-optimal sub-plans for later re-use.
	 * 
	 * @param query				the query being optimized
	 * @param plan				the query plan
	 * @param consideredMetric	Boolean flags indicating which cost metrics are considered
	 */
	void extractUsefulPlans(Query query, Plan plan, boolean[] consideredMetric) {
		// The following might introduce a cyclic reference but we clean it up at the end
		/*BitSet joinedTables = plan.resultRel.tableSet;
		Relation*/ 
		PruningUtil.prune(query, plan.resultRel, plan, 1, consideredMetric, true);
		if (plan instanceof JoinPlan) {
			JoinPlan joinPlan = (JoinPlan)plan;
			extractUsefulPlans(query, joinPlan.getLeftPlan(), consideredMetric);
			extractUsefulPlans(query, joinPlan.getRightPlan(), consideredMetric);
		}
	}
	/**
	 * Returns a plan whose result relations point to the ones in our partial plan cache.
	 * The cost of the cache-aware plan is not calculated - later functions will only those
	 * the join order specified by the cache-aware plan but not the plan itself.
	 * 
	 * @param query			query to optimize
	 * @param plan			a plan whose result relations are not in our cache
	 * @param algIndex		index of algorithm configuration within benchmark
	 * @param sizeIndex		index of query size within benchmark
	 * @param queryIndex	index of query within its query size group
	 * @return			a new plan whose result relations point towards our cache
	 */
	Plan cacheAwarePlan(Query query, Plan plan, int algIndex, int sizeIndex, int queryIndex) {
		if (plan instanceof ScanPlan) {
			ScanPlan scanPlan = (ScanPlan)plan;
			int tableIndex = scanPlan.tableIndex;
			ScanOperator scanOperator = scanPlan.scanOperator;
			Relation rel = getSingleTableRel(query, tableIndex, algIndex, sizeIndex, queryIndex);
			return new ScanPlan(rel, scanOperator);
		} else {
			assert(plan instanceof JoinPlan);
			JoinPlan joinPlan = (JoinPlan)plan;
			JoinOperator joinOperator = joinPlan.getJoinOperator();
			Plan leftPlan = cacheAwarePlan(query, joinPlan.getLeftPlan(), 
					algIndex, sizeIndex, queryIndex);
			Plan rightPlan = cacheAwarePlan(query, joinPlan.getRightPlan(), 
					algIndex, sizeIndex, queryIndex);
			Relation leftRel = leftPlan.resultRel;
			Relation rightRel = rightPlan.resultRel;
			Relation resultRel = getJoinRel(query, leftRel, rightRel, 
					algIndex, sizeIndex, queryIndex);
			return new JoinPlan(leftRel, rightRel, resultRel, leftPlan, rightPlan, joinOperator);
		}
	}
	/**
	 * Uses the join order of the given plan but varies the operators creating the operator skyline.
	 * Reuses all cached Pareto plans for each intermediate result generated by this plan. Inserts
	 * new Pareto plans into the corresponding plan caches if their cost is sufficiently different
	 * from the plans already contained. The approximation factor defines what is sufficiently
	 * different. Attention: this function expects that the cache has already been initialized
	 * for all relations that are generated during plan execution!
	 * 
	 * @param query				query to find plans for
	 * @param plan				a query plan from which we use the join order
	 * @param planSpace			determines the set of operators to consider
	 * @param costModel			used to calculate cost of different operator combinations
	 * @param consideredMetric	Boolean flags indicating which cost metrics are used
	 * @param alpha				coarsening factor for skyline approximation
	 */
	void operatorSkyline(Query query, Plan plan, PlanSpace planSpace, 
			MultiCostModel costModel, boolean[] consideredMetric, double alpha) {
		Relation resultRel = plan.resultRel;
		if (plan instanceof ScanPlan) {
			for (ScanOperator scanOperator : planSpace.scanOperatorsShuffled(resultRel)) {
				Plan newPlan = new ScanPlan(resultRel, scanOperator);
				costModel.updateRoot(newPlan);
				PruningUtil.prune(query, resultRel, newPlan, alpha, consideredMetric, false);
			}
		} else {
			assert(plan instanceof JoinPlan);
			JoinPlan joinPlan = (JoinPlan)plan;
			Relation leftRel = joinPlan.getLeftPlan().resultRel;
			Relation rightRel = joinPlan.getRightPlan().resultRel;
			operatorSkyline(query, joinPlan.getLeftPlan(), 
					planSpace, costModel, consideredMetric, alpha);
			operatorSkyline(query, joinPlan.getRightPlan(), 
					planSpace, costModel, consideredMetric, alpha);
			for (Plan leftPlan : leftRel.ParetoPlans) {
				for (Plan rightPlan : rightRel.ParetoPlans) {
					for (JoinOperator joinOperator : 
						planSpace.joinOperatorsShuffled(leftPlan, rightPlan)) {
						Plan newPlan = new JoinPlan(leftRel, rightRel, resultRel, 
								leftPlan, rightPlan, joinOperator);
						costModel.updateRoot(newPlan);
						PruningUtil.prune(query, resultRel, newPlan, alpha, consideredMetric, false);
					}
				}
			}
		}
		assert(resultRel.ParetoPlans.size() > 0);
	}
	/**
	 * Refining the approximation includes the following steps:
	 * 1. Generate a random join order.
	 * 2. Improve that join order via local search (only mutation join order but
	 * 		not the join and scan operator implementations).
	 * 3. Examine locally optimal join order and cache Pareto-optimal sub-plans.
	 * 4. Select the most interesting coarsening factor to approximate the Pareto frontier.
	 * 5. Generate Pareto frontier approximation with selected coarsening factor along 
	 * 		the intermediate results used by locally optimal join order using 
	 * 		Pareto-optimal plans cached for those intermediate results in prior 
	 * 		iterations and trying out different join operator implementations.
	 */
	@Override
	protected void refineApproximation(Query query,
			boolean[] consideredMetrics, PlanSpace planSpace,
			MultiCostModel costModel, 
			int algIndex, int sizeIndex, int queryIndex) {
		// Generate random plan joining all tables
		/*
		Plan randomPlan = randomJoinOrder(query, planSpace,  
				costModel, algIndex, sizeIndex);
		*/
		Plan randomPlan = LocalSearchUtil.randomBushyPlan(query, planSpace);
		costModel.updateAll(randomPlan);
		// Improve by local search
		Plan localOptimum = LocalSearchUtil.ParetoClimb(
				query, randomPlan, planSpace, costModel, consideredMetrics);
		// Cache useful sub-plans and thereby initialize relations 
		// extractUsefulPlans(query, localOptimum, consideredMetrics);
		// Make plan aware of the partial plan cache by pointing its result relations to it
		Plan cacheAwareOptimum = cacheAwarePlan(query, localOptimum, 
				algIndex, sizeIndex, queryIndex);
		costModel.updateAll(cacheAwareOptimum);
		//addToFrontier(query, cacheAwareOptimum, consideredMetrics);
		extractUsefulPlans(query, cacheAwareOptimum, consideredMetrics);
		// Continuously refine coarsening factor
		++nrRefinements;
		double coarseningFactor = Math.max(25 * Math.pow(0.99, nrRefinements/25), 1.0001);
		/*
		// Select coarsening factor with highest UCB value
		int coarseningIndex = maxUCBcoarsening();
		double coarseningFactor = coarseningFactors[coarseningIndex];
		// Update algorithm-specific statistics
		String featureName = "Selected " + coarseningIndex + " coarsening factor"; 
		Statistics.addToLongFeature(featureName, algIndex, sizeIndex, 0, 1);
		*/
		/*
		System.out.println("Rewards: " + Arrays.toString(accumulatedRewards));
		System.out.println("Played: " + Arrays.toString(nrPlayed));
		System.out.println("Selected: " + coarseningFactor);
		*/
		/*
		// Calculate operator skyline
		operatorSkyline(query, localOptimum, planSpace, 
				costModel, consideredMetrics, coarseningFactor);
		// Update factor statistics
		updateFactorStats(localOptimum.resultRel.ParetoPlans, coarseningIndex, consideredMetrics);
		// Set current skyline approximation to result relation Pareto plans
		for (Plan plan : localOptimum.resultRel.ParetoPlans) {
			addToFrontier(query, plan, consideredMetrics);
		}
		*/
		// Calculate operator skyline
		operatorSkyline(query, cacheAwareOptimum, planSpace, 
				costModel, consideredMetrics, coarseningFactor);
		// Update factor statistics
		//updateFactorStats(cacheAwareOptimum.resultRel.ParetoPlans, coarseningIndex, consideredMetrics);
		// Set current skyline approximation to result relation Pareto plans
		for (Plan plan : cacheAwareOptimum.resultRel.ParetoPlans) {
			addToFrontier(query, plan, consideredMetrics);
		}
	}
	/**
	 * Clean up cached partial plans for each intermediate result. We need to break up cyclic
	 * references between relations (pointing to optimal plans) and plans (pointing to the relation
	 * that they produce).
	 */
	public void cleanUp() {
		for (Entry<BitSet, Relation> entry : relations.entrySet()) {
			Relation rel = entry.getValue();
			if (rel.ParetoPlans != null) {
				rel.ParetoPlans.clear();				
			}
		}
	}
	
	@Override
	public String toString() {
		return "X(Cfact:" + Arrays.toString(coarseningFactors) + ")";
	}
	/**
	 * No algorithm-specific features.
	 */
	@Override
	protected void storeSpecificStatistics(int algIndex, int sizeIndex,
			int queryIndex) {
	}
}
