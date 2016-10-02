package optimizer.randomized.genetic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import common.RandomNumbers;
import cost.MultiCostModel;
import optimizer.randomized.RandomizedOptimizer;
import plans.Plan;
import plans.spaces.PlanSpace;
import queries.Query;

/**
 * Genetic algorithm for single-objective query optimization. We implement the genetic algorithm
 * for bushy plans that was described by Steinbrunn et al. in their VLDB'97 paper "Heuristic and
 * randomized optimization for the join ordering problem".
 * 
 * @author immanueltrummer
 *
 */
public class SoqoGA extends RandomizedOptimizer {
	/**
	 * The population of individuals, each individual represents one query plan.
	 */
	List<Individual> population;
	/**
	 * The number of individuals in the population (we use the number proposed by Steinbrunn et al.).
	 */
	final static int POPULATION_SIZE = 128;
	/**
	 * The probability that one gene gets mutated (we use the number proposed by Steinbrunn et al.).
	 */
	final static double MUTATION_PROBABILITY = 0.05;
	/**
	 * The probability that we perform a crossover between two individuals instead of just
	 * copying them into the next generation (we use the number proposed by Steinbrunn et al.).
	 */
	final static double CROSSOVER_RATE = 0.65;
	/**
	 * Select one individual from the population by ranking-based wheel selection.
	 * 
	 * @return	a randomly selected individual (the higher its fitness, 
	 * 			the higher the selection probability).
	 */
	Individual selectIndividual() {
		// Each individual is associated with an interval in the domain of the random
		// selection whose interval size is proportional to the individual's rank (the
		// better the rank, the larger the interval).
		int max = POPULATION_SIZE * (POPULATION_SIZE/2);
		int rand = RandomNumbers.random.nextInt(max);
		// Find out whose individual's interval we have hit
		int sum = POPULATION_SIZE;
		int intervalWidth = POPULATION_SIZE;
		int index = 0;
		while (rand >= sum) {
			intervalWidth -= 1;
			sum += intervalWidth;
			index += 1;
		}
		assert(index < POPULATION_SIZE) : "max: " + max + "; rand: " + rand + 
			"; sum: " + sum + "; intervalWidth: " + intervalWidth;
		return population.get(index);
	}
	/**
	 * Randomly selects two individuals from the current population and uses them
	 * to create two individuals for the next generation - either by a crossover 
	 * or by simply copying them.
	 * 
	 * @return	a list of two individuals to insert into the next generation
	 */
	List<Individual> newIndividuals() {
		List<Individual> newIndividuals = new LinkedList<Individual>();
		// Select parents randomly
		Individual parent1 = selectIndividual();
		Individual parent2 = selectIndividual();
		// Decide whether to copy or to use a crossover
		if (RandomNumbers.random.nextDouble() <= CROSSOVER_RATE) {
			// Add mutated offspring
			List<Individual> offspring = parent1.crossover(parent2);
			for (Individual singleOffspring : offspring) {
				newIndividuals.add(singleOffspring.mutated(MUTATION_PROBABILITY));				
			}
		} else {
			// Add mutated copies
			newIndividuals.add(parent1.mutated(MUTATION_PROBABILITY));
			newIndividuals.add(parent2.mutated(MUTATION_PROBABILITY));
		}
		return newIndividuals;
	}
	/**
	 * Fill up the new generation by repeating the following steps:
	 * - select two individuals by ranking based selection
	 * - either do a crossover or copy the two individuals into the next generation
	 * - mutate the two new (or old) individuals 
	 */
	@Override
	protected void refineApproximation(Query query,
			boolean[] consideredMetrics, PlanSpace planSpace,
			MultiCostModel costModel, int algIndex, int sizeIndex,
			int queryIndex) {
		// Order individuals in current generation by cost
		Collections.sort(population, new IndividualCostComparator(0));
		// Start building new generation
		List<Individual> newGeneration = new ArrayList<Individual>();
		// Iterate until the new generation is complete
		while (newGeneration.size() < POPULATION_SIZE) {
			newGeneration.addAll(newIndividuals());
		}
		// grow individuals in new generation
		for (Individual individual : newGeneration) {
			individual.grow();
		}
		population = newGeneration;
		// Determine best plan in current population
		Plan bestPlan = population.iterator().next().plan;
		for (Individual individual : population) {
			Plan newPlan = individual.plan;
			if (newPlan.cost[0] < bestPlan.cost[0]) {
				bestPlan = newPlan;
			}
		}
		// Insert best plan in current population into "frontier" approximation
		currentApproximation.clear();
		currentApproximation.add(bestPlan);
	}
	/**
	 * Initializes the population by randomly generated individuals.
	 */
	@Override
	protected void init(Query query, boolean[] consideredMetrics,
			PlanSpace planSpace, MultiCostModel costModel) {
		// We use an array list implementation as we have many random accesses
		population = new ArrayList<Individual>();
		// Generate the required number of individuals
		for (int individualCtr=0; individualCtr<POPULATION_SIZE; ++individualCtr) {
			Individual newIndividual = new Individual(query, planSpace, costModel);
			newIndividual.grow();
			population.add(newIndividual);
		}
	}
	/**
	 * Discards current population.
	 */
	@Override
	public void cleanUp() {
		population.clear();
	}
	@Override 
	public String toString() {
		return "SoqoGA";
	}
	/**
	 * No specific features to store statistics on.
	 */
	@Override
	protected void storeSpecificStatistics(int algIndex, int sizeIndex,
			int queryIndex) {
	}
}
