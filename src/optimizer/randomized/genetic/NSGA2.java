package optimizer.randomized.genetic;

import static common.Constants.*;
import static common.RandomNumbers.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cost.MultiCostModel;
import optimizer.randomized.RandomizedOptimizer;
import plans.spaces.PlanSpace;
import queries.Query;
import util.PruningUtil;

/**
 * Implementation of the NSGA-II genetic algorithm for multi-objective optimization.
 * Our implementation closely follows the pseudo-code that was given in the paper 
 * "A fast and elitist multiobjective genetic algorithm: NSGA-II" by Deb, Pratap, 
 * Agrawal et al., 2002.
 * 
 * @author immanueltrummer
 *
 */
public class NSGA2 extends RandomizedOptimizer {
	/**
	 * The number of individuals (representing query plans) in the population.
	 */
	final int NR_INDIVIDUALS;
	/**
	 * The probability that two parents generate offspring by crossover.
	 */
	final double CROSSOVER_PROBABILITY;
	/**
	 * The probability that one specific gene is mutated.
	 */
	double MUTATION_PROBABILITY = -1;
	/**
	 * Contains a set of query plans.
	 */
	List<Individual> population;
	
	public NSGA2(int NR_INDIVIDUALS, double CROSSOVER_PROBABILITY) {
		this.NR_INDIVIDUALS = NR_INDIVIDUALS;
		this.CROSSOVER_PROBABILITY = CROSSOVER_PROBABILITY;
	}
	/**
	 * Performs an efficient non-dominated sorting on current population
	 * as described in "A Fast and Elitist Multiobjective Genetic Algorithm:
	 * NSGA II". Returns hash map partitioning individuals by rank.
	 * 
	 * @param consideredMetric	Boolean flags indicating which metrics are used
	 * @return					A hash map mapping rank numbers to sets of individuals
	 */
	Map<Integer, ArrayList<Individual>> nonDominatedSort(boolean[] consideredMetric) {
		// Maps rank to individuals with that rank
		Map<Integer, ArrayList<Individual>> individualsByRank = 
				new HashMap<Integer, ArrayList<Individual>>();
		ArrayList<Individual> curRankIndividuals = new ArrayList<Individual>();
		// Find non-dominated individuals
		for (Individual p : population) {
			// Initialize counters
			p.dominatedIndividuals.clear();
			p.nrDominating = 0;
			// Compare cost against all other individuals
			double[] pCost = p.cost;
			for (Individual q : population) {
				double[] qCost = q.cost;
				if (PruningUtil.ParetoDominates(pCost, qCost, consideredMetric)) {
					p.dominatedIndividuals.add(q);
				} else if (PruningUtil.ParetoDominates(qCost, pCost, consideredMetric)) {
					p.nrDominating += 1;
				}
			}
			// Assign first rank
			if (p.nrDominating == 0) {
				p.rank = 1;
				curRankIndividuals.add(p);
			}
		}
		// Assign remaining ranks
		int i = 1;
		while (!curRankIndividuals.isEmpty()) {
			individualsByRank.put(i, curRankIndividuals);
			ArrayList<Individual> Q = new ArrayList<Individual>();
			for (Individual p : curRankIndividuals) {
				for (Individual q : p.dominatedIndividuals) {
					q.nrDominating -= 1;
					if (q.nrDominating == 0) {
						q.rank = i+1;
						Q.add(q);
					}
				}
			}
			i += 1;
			curRankIndividuals = Q;
		}
		return individualsByRank;
	}
	/**
	 * Calculate crowding distance for a list of individuals. The crowding distance
	 * is stored in the corresponding field of each individual.
	 * 
	 * @param individuals		individuals to calculate the crowding distance for
	 * @param consideredMetrics	Boolean flags indicating which metrics are relevant
	 */
	void calculateCrowdingDistance(ArrayList<Individual> individuals, 
			boolean[] consideredMetrics) {
		// Initialize crowding distance
		for (Individual i : individuals) {
			i.crowdingDistance = 0;
		}
		int nrIndividuals = individuals.size();
		// Calculate distance for each metric
		for (int m=0; m<NR_COST_METRICS; ++m) {
			if (consideredMetrics[m]) {
				Collections.sort(individuals, new IndividualCostComparator(m));
				Individual firstIndividual = individuals.get(0);
				Individual lastIndividual = individuals.get(nrIndividuals-1);
				firstIndividual.crowdingDistance = Double.POSITIVE_INFINITY;
				lastIndividual.crowdingDistance = Double.POSITIVE_INFINITY;
				double fMin = firstIndividual.cost[m];
				double fMax = lastIndividual.cost[m];
				assert(fMin <= fMax);
				for (int i=1; i<nrIndividuals-1; ++i) {
					// Add crowding distance for current cost metric
					double newCrowdingDistance =
							individuals.get(i).crowdingDistance +
							(individuals.get(i+1).cost[m] - individuals.get(i-1).cost[m])/
							(fMax-fMin);
					// Need to account for cases with infinite cost - set crowding distance
					// to infinite in those cases.
					if (Double.isNaN(newCrowdingDistance)) {
						newCrowdingDistance = Double.POSITIVE_INFINITY;
					}
					// Update crowding distance
					individuals.get(i).crowdingDistance = newCrowdingDistance;
					assert(!Double.isNaN(individuals.get(i).crowdingDistance)) :
						"fMax: " + fMax + "; fMin: " + fMin + 
						"; next individual cost: " + individuals.get(i+1).cost[m] +
						"; previous individual cost: "+ individuals.get(i-1).cost[m];
				}
			}
		}
	}
	/**
	 * This comparator compares individuals based on their rank and crowding distance.
	 * An individual is better than another one if it has lower rank or - if they have
	 * equivalent rank - if it has a higher crowding distance.
	 * 
	 * @author immanueltrummer
	 *
	 */
	class IndividualRankComparator implements Comparator<Individual> {
		@Override
		public int compare(Individual o1, Individual o2) {
			if (o1.betterThan(o2)) {
				return -1;
			} else if (o2.betterThan(o1)) {
				return 1;
			} else {
				return 0;
			}
		}
	}
	/**
	 * Fill parent population based on ranked individuals - calculates crowding distance
	 * for all layers that are inserted as parents. Given a set of individuals ranked by
	 * dominance, this function returns a subset of them that will generate offspring.
	 * We select a certain number of individuals by rank and use the crowding distance
	 * to prioritize between individuals of the same rank.
	 * 
	 * @param 	individualsByRank	individuals ordered by rank
	 * @param 	consideredMetrics	Boolean flags indicating which metrics are considered
	 * @return	a subset of individuals that form the parents for the next generation
	 */
	List<Individual> fillParents(Map<Integer, ArrayList<Individual>> individualsByRank,
			boolean[] consideredMetrics) {
		List<Individual> parents = new ArrayList<Individual>();
		// Insert individuals in ascending rank order as long as entire ranks fit into parents
		ArrayList<Individual> curRankIndividuals = individualsByRank.get(1);
		int i = 1;
		while (parents.size() + curRankIndividuals.size() <= NR_INDIVIDUALS) {
			calculateCrowdingDistance(curRankIndividuals, consideredMetrics);
			parents.addAll(curRankIndividuals);
			i += 1;
			curRankIndividuals = individualsByRank.get(i);
		}
		// Fill parents from current layer based on crowding distance
		Collections.sort(curRankIndividuals, new IndividualRankComparator());
		int nrParentSlots = NR_INDIVIDUALS - parents.size();
		parents.addAll(curRankIndividuals.subList(0, nrParentSlots));
		return parents;
	}
	/**
	 * Selects an individual from current population by performing a binary tournament.
	 * Two individuals are selected randomly and the better one of them is returned.
	 * 
	 * @return	the winner of a binary tournament between two randomly selected individuals.
	 */
	Individual binaryTournamendIndividual() {
		int curPopulationSize = population.size();
		int individual1Index = random.nextInt(curPopulationSize);
		int individual2Index = individual1Index;
		while (individual2Index == individual1Index) {
			individual2Index = random.nextInt(curPopulationSize);
		}
		Individual individual1 = population.get(individual1Index);
		Individual individual2 = population.get(individual2Index);
		if (individual1.betterThan(individual2)) {
			return individual1;
		} else if (individual2.betterThan(individual1)) {
			return individual2;
		} else {
			return random.nextBoolean() ? individual1 : individual2;
		}
	}
	/**
	 * Generates offspring from the given parents - assumes that rank and crowding distance
	 * have been calculated for all parents. The offspring is already grown (i.e., the plans
	 * and corresponding cost values are calculated for the given genes).
	 * 
	 * @param parents	the parent individuals from which to generate offspring
	 * @return			a set of offspring individuals generated from parents
	 */
	List<Individual> generateOffspring(List<Individual> parents) {
		List<Individual> offspring = new ArrayList<Individual>();
		// Generate offspring
		for (int offspringCtr=0; offspringCtr<NR_INDIVIDUALS/2; ++offspringCtr) {
			Individual parent1 = binaryTournamendIndividual();
			Individual parent2 = binaryTournamendIndividual();
			if (random.nextDouble()<=CROSSOVER_PROBABILITY) {
				offspring.addAll(parent1.crossover(parent2));
			} else {
				Individual parent1Clone = new Individual(
						parent1.query, parent1.planSpace, parent1.costModel, parent1.genes);
				Individual parent2Clone = new Individual(
						parent2.query, parent2.planSpace, parent2.costModel, parent2.genes);
				offspring.add(parent1Clone);
				offspring.add(parent2Clone);
			}
		}
		// Mutate offspring
		List<Individual> mutatedOffspring = new ArrayList<Individual>();
		for (Individual child : offspring) {
			Individual mutatedChild = child.mutated(MUTATION_PROBABILITY);
			mutatedOffspring.add(mutatedChild);
		}
		return mutatedOffspring;
	}
	/**
	 * Clears current frontier approximation and empties population. Sets mutation
	 * probability based on the number of genes.
	 */
	@Override
	protected void init(Query query, boolean[] consideredMetrics,
			PlanSpace planSpace, MultiCostModel costModel) {
		population = new ArrayList<Individual>();
		int nrGenes = query.nrTables - 1;
		MUTATION_PROBABILITY = 1.0/nrGenes;
	}

	@Override
	protected void refineApproximation(Query query,
			boolean[] consideredMetrics, PlanSpace planSpace,
			MultiCostModel costModel, int algIndex, int sizeIndex, int queryIndex) {
		// Need to initialize population if empty
		if (population.isEmpty()) {
			for (int individualCtr=0; individualCtr<2*NR_INDIVIDUALS; ++individualCtr) {
				Individual newIndividual = new Individual(query, planSpace, costModel);
				newIndividual.grow();
				population.add(newIndividual);
			}
		} else {
			// Non-dominated sort
			Map<Integer, ArrayList<Individual>> individualsByRank = 
					nonDominatedSort(consideredMetrics);
			// Fill parent population
			List<Individual> P = fillParents(individualsByRank, consideredMetrics);
			// Generate offspring and grow
			List<Individual> Q = generateOffspring(P);
			for (Individual child : Q) {
				child.grow();
			}
			// Combine parent and offspring into population
			population.clear();
			population.addAll(P);
			population.addAll(Q);
		}
		// Current population forms Pareto frontier approximation
		currentApproximation.clear();
		for (Individual individual : population) {
			currentApproximation.add(individual.plan);
		}
	}

	@Override
	public void cleanUp() {
		population.clear();
	}
	
	@Override
	public String toString() {
		return "GA(NR_INDIVIDUALS=" + NR_INDIVIDUALS 
		+ ", CROSSOVER_PROBABILITY=" + CROSSOVER_PROBABILITY
		+ ", MUTATION_PROBABILITY=" + MUTATION_PROBABILITY;
	}
	/**
	 * No specific features to store.
	 */
	@Override
	protected void storeSpecificStatistics(int algIndex, int sizeIndex,
			int queryIndex) {
	}

}
