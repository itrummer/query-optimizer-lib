package optimizer.randomized.genetic;

import static org.junit.Assert.*;
import static util.TestUtil.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import cost.MultiCostModel;
import cost.SingleCostModel;
import cost.local.TimeCostModel;
import queries.JoinGraphType;
import queries.JoinType;
import queries.Query;
import queries.QueryFactory;

import org.junit.Test;

public class SoqoGATest {

	@Test
	public void test() {
		// Create scalar cost model with execution time as only metric
		MultiCostModel costModel = new MultiCostModel(
				Arrays.asList(new SingleCostModel[] {new TimeCostModel(0)}));
		boolean[] consideredMetrics = new boolean[] {true};
		// Initialization
		{
			// We must have 128 individuals after initialization
			Query query = QueryFactory.produceSteinbrunn(JoinGraphType.CHAIN, 50, JoinType.MIN);
			SoqoGA ga = new SoqoGA();
			ga.init(query, consideredMetrics, planSpace, costModel);
			assertEquals(128, ga.population.size());
		}
		// Selection of individuals
		{
			// The probability of an individual to get selected is proportional to its rank.
			Query query = QueryFactory.produceSteinbrunn(JoinGraphType.CHAIN, 50, JoinType.MIN);
			SoqoGA ga = new SoqoGA();
			ga.init(query, consideredMetrics, planSpace, costModel);
			// Mark individuals in the population
			for (int i=0; i<128; ++i) {
				ga.population.get(i).nrDominating = i;
			}
			// Execute many selections and verify that number of selections is proportional to the rank
			int[] nrSelected = new int[128];
			for (int i=0; i<1000000; ++i) {
				Individual individual = ga.selectIndividual();
				++nrSelected[individual.nrDominating];
			}
			assertTrue(nrSelected[0] > nrSelected[127]);
			assertTrue(nrSelected[1] > nrSelected[126]);
		}
		// Generation of individuals for next iteration
		{
			// If the population consists of identical individuals then the new individuals do, too
			for (int i=0; i<10; ++i) {
				Query query = QueryFactory.produceSteinbrunn(JoinGraphType.CHAIN, 50, JoinType.MIN);
				SoqoGA ga = new SoqoGA();
				ga.population = new ArrayList<Individual>();
				Individual templateIndividual = new Individual(query, planSpace, costModel);
				for (int j=0; j<128; ++j) {
					Individual individual = new Individual(query, planSpace, costModel, templateIndividual.genes);
					ga.population.add(individual);
				}
				List<Individual> newIndividuals = ga.newIndividuals();
				for (Individual newIndividual : newIndividuals) {
					assertArrayEquals(templateIndividual.genes, newIndividual.genes);
					assertArrayEquals(templateIndividual.genes, newIndividual.genes);
				}				
			}
		}
		// Refining the approximation
		{
			Query query = QueryFactory.produceSteinbrunn(JoinGraphType.CHAIN, 50, JoinType.MIN);
			SoqoGA ga = new SoqoGA();
			ga.init(query, consideredMetrics, planSpace, costModel);
			ga.refineApproximation(query, consideredMetrics, planSpace, costModel, 0, 0, 0);
			// Make sure that new population has required number of individuals
			assertEquals(128, ga.population.size());
		}
	}

}
