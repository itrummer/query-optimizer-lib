package optimizer.randomized.genetic;

import static common.RandomNumbers.*;
import static common.Constants.*;
import cost.MultiCostModel;
import plans.JoinPlan;
import plans.Plan;
import plans.ScanPlan;
import plans.operators.JoinOperator;
import plans.operators.ScanOperator;
import plans.spaces.PlanSpace;
import queries.Query;
import relations.Relation;
import relations.RelationFactory;
import util.TestUtil;

import java.util.LinkedList;
import java.util.List;

/**
 * An individual within the population set of the genetic algorithm. Each individual 
 * represents one query plan. This class is used by all genetic algorithms, some of
 * its fields and methods are however only used by the NSGA-2 version of the genetic 
 * algorithm.
 * 
 * @author immanueltrummer
 *
 */
public class Individual {
	/**
	 * The query that is being optimized.
	 */
	final Query query;
	/**
	 * The plan space that is considered during optimization.
	 */
	final PlanSpace planSpace;
	/**
	 * Estimates the execution cost of plans.
	 */
	final MultiCostModel costModel;
	/**
	 * The genes describe an individual by a list of join pairs, describing which operands
	 * are joined and by which operator.
	 */
	final JoinPair[] genes;
	/**
	 * Query plan corresponding to the genes.
	 */
	Plan plan;
	/**
	 * The execution cost of the query plan represented by this individual.
	 */
	double[] cost;
	/**
	 * The rank is used by NSGA-2 to compare individuals. A lower rank is preferable
	 * and the rank is based on the number of other individuals in the same generatio
	 * which dominate this individual.
	 */
	public int rank;
	/**
	 * The number of individuals in the current generation that dominate this individual.
	 * Only used by NSGA-2.
	 */
	public int nrDominating;
	/**
	 * A list of individuals in the current generation which dominate this individual.
	 * Only used by NSGA-2.
	 */
	public List<Individual> dominatedIndividuals = new LinkedList<Individual>();
	/**
	 * The crowding distance is higher the less similar the cost vectors of other individuals
	 * in the current generation are to the cost vector of this individual. Only used by NSGA-2.
	 */
	public double crowdingDistance;
	
	/**
	 * This constructor generates genes randomly for the given number of tables.
	 * 
	 * @param query			the query being optimized
	 * @param planSpace		the plan space considered during optimization
	 * @param costModel		the cost model used to compare plans
	 */
	public Individual(Query query, PlanSpace planSpace, MultiCostModel costModel) {
		this.query = query;
		this.planSpace = planSpace;
		this.costModel = costModel;
		// Randomly initialize genes
		int nrJoins = query.nrTables - 1;
		genes = new JoinPair[nrJoins];
		for (int joinCtr=0; joinCtr<nrJoins; ++joinCtr) {
			genes[joinCtr] = randomJoinPair(joinCtr);
		}
	}
	/**
	 * This constructor generates individual based on given genes.
	 * 
	 * @param query			the query being optimized
	 * @param planSpace		the plan space considered during optimization
	 * @param costModel		the cost model used to compare plans
	 * @param genes			the new individual copies those genes
	 */
	public Individual(Query query, PlanSpace planSpace, 
			MultiCostModel costModel, JoinPair[] genes) {
		this.query = query;
		this.planSpace = planSpace;
		this.costModel = costModel;
		this.genes = genes;
	}
	/**
	 * Returns a randomly generated join pair.
	 * 
	 * @param nrPriorJoins	the number of joins executed before the join corresponding to
	 * 						this join pair is executed. The number of joins taken before
	 * 						determines the admissible value range for the operand indices.
	 * @return				a randomly generated join pair
	 */
	JoinPair randomJoinPair(int nrPriorJoins) {
		// Make mutation
		int nrTables = query.nrTables;
		int nrOperands = nrTables - nrPriorJoins;
		List<Integer> operatorIndices = new LinkedList<Integer>();
		for (int i=0; i<nrOperands; ++i) {
			operatorIndices.add(i);
		}
		int leftOperand = operatorIndices.remove(random.nextInt(nrOperands));
		int rightOperand = operatorIndices.remove(random.nextInt(nrOperands-1));
		int nrJoinOperators = planSpace.consideredJoinOps.size();
		int preferredJoin = random.nextInt(nrJoinOperators);
		return new JoinPair(leftOperand, rightOperand, preferredJoin);
	}
	
	// Crossover between this individual and a second parent
	/**
	 * Performs a crossover between this individual and the given partner and returns
	 * the resulting offspring. We use the point-wise crossover as described by 
	 * Steinbrunn et al. in their VLDB'97 paper. 
	 * 
	 * @param partner	the other parent with whom to perform the crossover
	 * @return			a list of two individuals that result from the crossover
	 */
	public List<Individual> crossover(Individual partner) {
		assert(genes.length == partner.genes.length);
		int geneLength = genes.length;
		JoinPair[] genes1 = new JoinPair[geneLength];
		JoinPair[] genes2 = new JoinPair[geneLength];
		int crossoverPoint = random.nextInt(geneLength);
		for (int geneCtr=0; geneCtr<geneLength; ++geneCtr) {
			if (geneCtr<=crossoverPoint) {
				genes1[geneCtr] = this.genes[geneCtr];
				genes2[geneCtr] = partner.genes[geneCtr];
			} else {
				genes1[geneCtr] = partner.genes[geneCtr];
				genes2[geneCtr] = this.genes[geneCtr];
			}
		}
		List<Individual> offspring = new LinkedList<Individual>();
		offspring.add(new Individual(query, planSpace, costModel, genes1));
		offspring.add(new Individual(query, planSpace, costModel, genes2));
		return offspring;
	}
	/**
	 * Mutates each gene of this individual with the given mutation probability
	 * and returns a newly created mutated individual.
	 * 
	 * @param mutationProbability	the probability that one specific gene is mutated
	 * @return						a mutated version of this individual
	 */
	public Individual mutated(double mutationProbability) {
		assert(mutationProbability >= 0);
		int geneLength = genes.length;
		JoinPair[] mutatedGenes = new JoinPair[geneLength];
		for (int joinCtr=0; joinCtr<geneLength; ++joinCtr) {
			if (random.nextDouble() > mutationProbability) {
				// No mutation
				mutatedGenes[joinCtr] = genes[joinCtr];
			} else {
				mutatedGenes[joinCtr] = randomJoinPair(joinCtr);
			}
		}
		return new Individual(query, planSpace, costModel, mutatedGenes);
	}
	/**
	 * Generates the query plan that is described by the genes and calculates its cost.
	 * 
	 */
	public void grow() {
		assert(plan == null);
		List<Plan> partialPlans = new LinkedList<Plan>();
		// Initialize partial plans with single table scans
		int nrTables = query.nrTables;
		for (int tableIndex=0; tableIndex<nrTables; ++tableIndex) {
			Relation rel = RelationFactory.createSingleTableRel(query, tableIndex);
			// For the moment there is only one scan operator - once 
			// that changes, we should store the scan operator in the genes
			ScanOperator scanOperator = planSpace.randomScanOperator(rel);
			ScanPlan newPlan = new ScanPlan(rel, scanOperator);
			partialPlans.add(newPlan);
		}
		assert(partialPlans.size() == nrTables);
		// Join partial plans as specified in the genes
		int nrJoins = nrTables - 1;
		for (int joinCtr=0; joinCtr<nrJoins; ++joinCtr) {
			JoinPair joinPair = genes[joinCtr];
			int leftIndex = joinPair.leftOperand;
			int rightIndex = joinPair.rightOperand;
			Plan leftPlan = partialPlans.get(leftIndex);
			Plan rightPlan = partialPlans.get(rightIndex);
			// Must remove elements starting from higher index
			partialPlans.remove(Math.max(leftIndex, rightIndex));
			partialPlans.remove(Math.min(leftIndex, rightIndex));
			int preferredJoinOpIndex = joinPair.preferredOperator;
			List<JoinOperator> consideredJoins = planSpace.consideredJoinOps;
			JoinOperator joinOp = consideredJoins.get(preferredJoinOpIndex);
			if (!planSpace.joinOperatorApplicable(joinOp, leftPlan, rightPlan)) {
				joinOp = planSpace.randomJoinOperator(leftPlan, rightPlan);
			}
			Plan newPartialPlan = new JoinPlan(query, leftPlan, rightPlan, joinOp);
			partialPlans.add(0, newPartialPlan);
		}
		// Should now have only one plan left
		assert(partialPlans.size() == 1);
		plan = partialPlans.get(0);
		costModel.updateAll(plan);
		plan.makeImmutable();
		if (SAFE_MODE) {
			TestUtil.validatePlan(plan, planSpace, costModel, true);
		}
		cost = plan.getCostValuesCopy();
	}
	/**
	 * Compares this individual against another individual based on rank and crowding distance.
	 * This method is only used by NSGA-2.
	 * 
	 * @param otherIndividual	the individual against which we compare
	 * @return					Boolean indicating whether this individual is better than the other one
	 */
	public boolean betterThan(Individual otherIndividual) {
		if (rank < otherIndividual.rank) {
			return true;
		} else if (rank == otherIndividual.rank && 
				crowdingDistance > otherIndividual.crowdingDistance){
			return true;
		} else {
			return false;			
		}
	}
}
