package optimizer.randomized.genetic;

import static org.junit.Assert.*;
import static util.TestUtil.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import queries.JoinGraphType;
import queries.JoinType;
import queries.Query;
import queries.QueryFactory;

import org.junit.Test;

public class NSGA2Test {

	@Test
	public void test() {
		// Rank assignments by non-dominated sort
		{
			Query query = QueryFactory.produceSteinbrunn(JoinGraphType.STAR, 6, JoinType.MN);
			NSGA2 ga = new NSGA2(100, 0.9);
			boolean[] allMetrics = new boolean[] {true, true, true};
			ga.population = new ArrayList<Individual>();
			for (int i=0; i<4; ++i) {
				ga.population.add(new Individual(query, planSpace, costModel));				
			}
			ga.population.get(0).cost = new double[] {5, 5, 5};
			ga.population.get(1).cost = new double[] {10, 10, 10};
			ga.population.get(2).cost = new double[] {1, 7, 7};
			ga.population.get(3).cost = new double[] {3, 8, 8};
			Map<Integer, ArrayList<Individual>> byRank = ga.nonDominatedSort(allMetrics);
			// Check rank-layer cardinality
			assertEquals(3, byRank.size());
			assertEquals(2, byRank.get(1).size());
			assertEquals(1, byRank.get(2).size());
			assertEquals(1, byRank.get(3).size());
			// Check that rank fields are set consistently
			ArrayList<Individual> rank1 = byRank.get(1);
			ArrayList<Individual> rank2 = byRank.get(2);
			ArrayList<Individual> rank3 = byRank.get(3);
			for (Individual individual : rank1) {
				assertEquals(1, individual.rank);
			}
			for (Individual individual : rank2) {
				assertEquals(2, individual.rank);
			}
			for (Individual individual : rank3) {
				assertEquals(3, individual.rank);
			}
			// Check that ranks contain the right individuals
			assertEquals(1, ga.population.get(0).rank);
			assertEquals(1, ga.population.get(2).rank);
			assertEquals(2, ga.population.get(3).rank);
			assertEquals(3, ga.population.get(1).rank);
		}
		// Compare individuals based on cost
		{
			Query query = QueryFactory.produceSteinbrunn(JoinGraphType.STAR, 6, JoinType.MN);
			Individual individual1 = new Individual(query, planSpace, costModel);
			Individual individual2 = new Individual(query, planSpace, costModel);
			Individual individual3 = new Individual(query, planSpace, costModel);
			Individual individual4 = new Individual(query, planSpace, costModel);
			individual1.cost = new double[] {1, 3, 2};
			individual2.cost = new double[] {5, 3, 5};
			individual3.cost = new double[] {10, 1, 1};
			individual4.cost = new double[] {2, 2, 2};
			IndividualCostComparator cost0Comp = 
					new IndividualCostComparator(0);
			IndividualCostComparator cost1Comp = 
					new IndividualCostComparator(1);
			IndividualCostComparator cost2Comp = 
					new IndividualCostComparator(2);
			assertTrue(cost0Comp.compare(individual1, individual2) < 0);
			assertTrue(cost0Comp.compare(individual3, individual4) > 0);
			assertTrue(cost2Comp.compare(individual1, individual4) == 0);
			assertTrue(cost1Comp.compare(individual1, individual2) == 0);
		}
		// Calculation of crowding distance
		{
			Query query = QueryFactory.produceSteinbrunn(JoinGraphType.CHAIN, 6, JoinType.MN);
			Individual individual1 = new Individual(query, planSpace, costModel);
			Individual individual2 = new Individual(query, planSpace, costModel);
			Individual individual3 = new Individual(query, planSpace, costModel);
			Individual individual4 = new Individual(query, planSpace, costModel);
			ArrayList<Individual> individuals = new ArrayList<Individual>();
			individuals.add(individual1);
			individuals.add(individual2);
			individuals.add(individual3);
			individuals.add(individual4);
			NSGA2 ga = new NSGA2(100, 0.9);
			{
				// Check that individuals with minimal or maximal cost have infinite distance
				individual1.cost = new double[] {1, 3, 3};
				individual2.cost = new double[] {10, 3, 5};
				individual3.cost = new double[] {10, 2, 2};
				individual4.cost = new double[] {3, 2.5, 3};
				boolean[] allMetrics = new boolean[] {true, true, true};
				ga.calculateCrowdingDistance(individuals, allMetrics);
				assertEquals(Double.POSITIVE_INFINITY, individual1.crowdingDistance, EPSILON);
				assertEquals(Double.POSITIVE_INFINITY, individual2.crowdingDistance, EPSILON);
				assertEquals(Double.POSITIVE_INFINITY, individual3.crowdingDistance, EPSILON);
				assertNotEquals(Double.POSITIVE_INFINITY, individual4.crowdingDistance, EPSILON);
			}
			{
				// Make sure that only selected cost metrics are taken into account
				individual1.cost = new double[] {1, 3, 3};
				individual2.cost = new double[] {10, 3, 5};
				individual3.cost = new double[] {5, 2, 2};
				individual4.cost = new double[] {3, 2.5, 3};
				boolean[] firstMetric = new boolean[] {true, false, false};
				ga.calculateCrowdingDistance(individuals, firstMetric);
				assertEquals(Double.POSITIVE_INFINITY, individual1.crowdingDistance, EPSILON);
				assertEquals(Double.POSITIVE_INFINITY, individual2.crowdingDistance, EPSILON);
				assertNotEquals(Double.POSITIVE_INFINITY, individual3.crowdingDistance, EPSILON);
				assertNotEquals(Double.POSITIVE_INFINITY, individual4.crowdingDistance, EPSILON);

			}
			{
				// Individuals with cost closer to other individuals must have lower distance
				individual1.cost = new double[] {1, 1, 1};
				individual2.cost = new double[] {10, 10, 9};
				individual3.cost = new double[] {2, 1.3, 2};
				individual4.cost = new double[] {5, 7, 3};
				boolean[] allMetrics = new boolean[] {true, true, true};
				ga.calculateCrowdingDistance(individuals, allMetrics);
				assertTrue(individual3.crowdingDistance < individual4.crowdingDistance);
			}
		}
		// Compare individuals based on rank and crowding distance
		{
			NSGA2 ga = new NSGA2(100, 0.9);
			Query query = QueryFactory.produceSteinbrunn(JoinGraphType.CHAIN, 6, JoinType.MN);
			Individual individual1 = new Individual(query, planSpace, costModel);
			Individual individual2 = new Individual(query, planSpace, costModel);
			Individual individual3 = new Individual(query, planSpace, costModel);
			Individual individual4 = new Individual(query, planSpace, costModel);
			individual1.cost = new double[] {1, 1, 1};
			individual2.cost = new double[] {10, 10, 9};
			individual3.cost = new double[] {2, 1.3, 2};
			individual4.cost = new double[] {5, 7, 1.5};
			ga.population = new ArrayList<Individual>();
			ga.population.add(individual1);
			ga.population.add(individual2);
			ga.population.add(individual3);
			ga.population.add(individual4);
			boolean[] allMetrics = new boolean[] {true, true, true};
			ga.nonDominatedSort(allMetrics);
			ga.calculateCrowdingDistance((ArrayList<Individual>)ga.population, allMetrics);
			assertTrue(individual3.rank == individual4.rank);
			assertTrue(individual4.crowdingDistance > individual3.crowdingDistance);
			NSGA2.IndividualRankComparator rankCompare = 
					ga.new IndividualRankComparator();
			assertTrue(rankCompare.compare(individual1, individual2) < 0);
			assertTrue(rankCompare.compare(individual2, individual4) > 0);
			assertTrue(rankCompare.compare(individual3, individual4) > 0);
		}
		// Fill parent population
		{
			NSGA2 ga = new NSGA2(100, 0.9);
			Query query = QueryFactory.produceSteinbrunn(JoinGraphType.CHAIN, 6, JoinType.MN);
			ga.population = new ArrayList<Individual>();
			for (int individualCtr=0; individualCtr<2*ga.NR_INDIVIDUALS; ++individualCtr) {
				Individual newIndividual = new Individual(query, planSpace, costModel);
				newIndividual.grow();
				ga.population.add(newIndividual);
			}
			boolean[] allMetrics = new boolean[] {true, true, true};
			Map<Integer, ArrayList<Individual>> byRank = ga.nonDominatedSort(allMetrics);
			ga.calculateCrowdingDistance((ArrayList<Individual>)ga.population, allMetrics);
			List<Individual> parents = ga.fillParents(byRank, allMetrics);
			// Assert number of parents
			assertEquals(ga.NR_INDIVIDUALS, parents.size());
			// Retrieve non-parent individuals
			List<Individual> nonParents = new ArrayList<Individual>();
			nonParents.addAll(ga.population);
			nonParents.removeAll(parents);
			assertEquals(ga.NR_INDIVIDUALS, nonParents.size());
			// Assert that parents are better or equally good as non-parent
			Set<Integer> parentRanks = new TreeSet<Integer>();
			Set<Integer> nonParentRanks = new TreeSet<Integer>();
			for (Individual parent : parents) {
				parentRanks.add(parent.rank);
			}
			for (Individual nonParent : nonParents) {
				nonParentRanks.add(nonParent.rank);
			}
			int parentMaxRank = Collections.max(parentRanks);
			int nonParentMinRank = Collections.min(nonParentRanks);
			assertTrue(parentMaxRank <= nonParentMinRank);
			// Compare rank and crowding distance together
			NSGA2.IndividualRankComparator rankCompare = 
					ga.new IndividualRankComparator();
			for (Individual parent : parents) {
				for (Individual nonParent : nonParents) {
					assertTrue(rankCompare.compare(parent, nonParent) <= 0);
				}
			}
		}
		// Binary tournament
		{
			// Make sure that binary tournament never selects the worse individual
			NSGA2 ga = new NSGA2(100, 0.9);
			Query query = QueryFactory.produceSteinbrunn(JoinGraphType.CHAIN, 6, JoinType.MN);
			ga.population = new ArrayList<Individual>();
			Individual individual1 = new Individual(query, planSpace, costModel);
			Individual individual2 = new Individual(query, planSpace, costModel);
			individual1.cost = new double[] {1, 1, 1};
			individual2.cost = new double[] {10, 10, 9};
			ga.population.add(individual1);
			ga.population.add(individual2);
			boolean[] allMetrics = new boolean[] {true, true, true};
			ga.nonDominatedSort(allMetrics);
			ga.calculateCrowdingDistance((ArrayList<Individual>)ga.population, allMetrics);
			for (int i=0; i<50; ++i) {
				Individual winner = ga.binaryTournamendIndividual();
				assertSame(individual1, winner);
			}
		}
		// Generate offspring
		{
			NSGA2 ga = new NSGA2(100, 0.9);
			Query query = QueryFactory.produceSteinbrunn(JoinGraphType.CHAIN, 6, JoinType.MN);
			ga.population = new ArrayList<Individual>();
			for (int individualCtr=0; individualCtr<2*ga.NR_INDIVIDUALS; ++individualCtr) {
				Individual newIndividual = new Individual(query, planSpace, costModel);
				newIndividual.grow();
				ga.population.add(newIndividual);
			}
			boolean[] allMetrics = new boolean[] {true, true, true};
			Map<Integer, ArrayList<Individual>> byRank = ga.nonDominatedSort(allMetrics);
			assertNotNull(byRank);
			ga.calculateCrowdingDistance((ArrayList<Individual>)ga.population, allMetrics);
			List<Individual> parents = ga.fillParents(byRank, allMetrics);
			List<Individual> offspring = ga.generateOffspring(parents);
			assertEquals(ga.NR_INDIVIDUALS, offspring.size());
		}
	}

}
