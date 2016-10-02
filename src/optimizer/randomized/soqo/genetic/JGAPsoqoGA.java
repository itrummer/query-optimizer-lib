package optimizer.randomized.soqo.genetic;

import java.util.Arrays;
import java.util.List;

import org.jgap.Chromosome;
import org.jgap.Configuration;
import org.jgap.DefaultFitnessEvaluator;
import org.jgap.Gene;
import org.jgap.Genotype;
import org.jgap.IChromosome;
import org.jgap.impl.DefaultConfiguration;
import org.jgap.impl.IntegerGene;

import common.Constants;
import cost.MultiCostModel;
import optimizer.Optimizer;
import plans.ParetoPlanSet;
import plans.Plan;
import plans.spaces.PlanSpace;
import queries.Query;

/**
 * Genetic algorithm implementation based on the JGAP framework.
 * Only applicable for single-objective query optimization.
 * 
 * @author immanueltrummer
 *
 */
public class JGAPsoqoGA extends Optimizer {
	/**
	 * Used to verify class version.
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * The number of individuals in each generation.
	 */
	final int populationSize;
	
	public JGAPsoqoGA(int populationSize) {
		this.populationSize = populationSize;
	}
	
	@Override
	public ParetoPlanSet approximateParetoSet(Query query,
			boolean[] consideredMetrics, PlanSpace planSpace,
			MultiCostModel costModel, ParetoPlanSet refPlanSet, int algIndex,
			int sizeIndex, int queryIndex) {
		try {
			// Configure genetic algorithm
			Configuration.reset();
			Configuration gaConfiguration = new DefaultConfiguration();
			gaConfiguration.setPreservFittestIndividual(true);
			// Set fitness function
			PlanFitness fitnessFunction = new PlanFitness(query, planSpace, costModel);
			gaConfiguration.setFitnessFunction(fitnessFunction);
			// Set sample chromosome
			int nrTables = query.nrTables;
			int nrGenes = query.nrTables;
			Gene[] sampleGenes = new Gene[nrGenes];
			for (int geneCtr=0; geneCtr<nrGenes; ++geneCtr) {
				sampleGenes[geneCtr] = new IntegerGene(gaConfiguration, 0, nrTables - 1 - geneCtr);
			}
			IChromosome sampleChromosome = new Chromosome(gaConfiguration, sampleGenes);
			gaConfiguration.setSampleChromosome(sampleChromosome);
			// Set population size
			gaConfiguration.setPopulationSize(populationSize);
			// Generate population
			Genotype population = Genotype.randomInitialGenotype(gaConfiguration);
			// Obtain timing parameters for this run
			int nrPeriods = Constants.NR_TIME_PERIODS;
			long periodMillis = Constants.TIME_PERIOD_MILLIS;
			// Evolve
			IChromosome bestChromosome = null;
			Plan bestPlan = null;
			for (int periodCtr=0; periodCtr<nrPeriods; ++periodCtr) {
				long startMillis = System.currentTimeMillis();
				while (System.currentTimeMillis() - startMillis < periodMillis) {
					population.evolve();
				}
				bestChromosome = population.getFittestChromosome();
				bestPlan = fitnessFunction.extractPlan(bestChromosome);
				double bestCost = bestPlan.cost[0];
				System.out.println("Best cost after period " + periodCtr + ": " + bestCost);
			}
			// Wrap best plan into Pareto plan set and return
			List<Plan> planList = Arrays.asList(new Plan[] {bestPlan});
			return new ParetoPlanSet(planList);
		} catch (Exception e) {
			e.printStackTrace(System.out);
			return null;
		}
	}

}
