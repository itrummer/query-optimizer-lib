package optimizer.randomized.genetic;

import static org.junit.Assert.*;
import static util.TestUtil.*;
import static common.RandomNumbers.*;

import java.util.List;

import queries.JoinGraphType;
import queries.JoinType;
import queries.Query;
import queries.QueryFactory;
import util.TestUtil;

import org.junit.Test;

public class IndividualTest {
	
	// Checks whether two join pairs are equivalent
	boolean equivalentJoinPairs(JoinPair joinPair1, JoinPair joinPair2) {
		if (joinPair1.leftOperand == joinPair2.leftOperand &&
				joinPair1.rightOperand == joinPair2.rightOperand &&
				joinPair1.preferredOperator == joinPair2.preferredOperator) {
			return true;
		} else {
			return false;
		}
	}

	@Test
	public void test() {
		// Generating random join pairs
		{
			for (int i=0; i<50; ++i) {
				Query query = QueryFactory.produceSteinbrunn(JoinGraphType.CHAIN, 4, JoinType.MN);
				Individual individual = new Individual(query, planSpace, costModel);
				assertEquals(3, individual.genes.length);
				for (int joinCtr=0; joinCtr<3; ++joinCtr) {
					JoinPair joinPair = individual.genes[joinCtr];
					assertFalse(joinPair.leftOperand == joinPair.rightOperand);
					assertTrue(joinPair.leftOperand >= 0);
					assertTrue(joinPair.rightOperand >= 0);
					assertTrue(joinPair.preferredOperator < planSpace.consideredJoinOps.size());
				}
				assertTrue(individual.genes[0].leftOperand < 4);
				assertTrue(individual.genes[0].rightOperand < 4);
				assertTrue(individual.genes[1].leftOperand < 3);
				assertTrue(individual.genes[1].rightOperand < 3);
				assertTrue(individual.genes[2].leftOperand < 2);
				assertTrue(individual.genes[2].rightOperand < 2);
			}
		}
		// Crossover
		{
			for (int i=0; i<50; ++i) {
				Query query = QueryFactory.produceSteinbrunn(JoinGraphType.CHAIN, 7, JoinType.MN);
				Individual individual1 = new Individual(query, planSpace, costModel);
				Individual individual2 = new Individual(query, planSpace, costModel);
				List<Individual> offspring = individual1.crossover(individual2);
				assertEquals(2, offspring.size());
				Individual offspring1 = offspring.get(0);
				Individual offspring2 = offspring.get(1);
				for (int geneCtr=0; geneCtr<6; ++geneCtr) {
					boolean admissibleCopy = false;
					JoinPair individual1Join = individual1.genes[geneCtr];
					JoinPair individual2Join = individual2.genes[geneCtr];
					JoinPair offspring1Join = offspring1.genes[geneCtr];
					JoinPair offspring2Join = offspring2.genes[geneCtr];
					if (individual1Join.leftOperand == offspring1Join.leftOperand &&
							individual1Join.rightOperand == offspring1Join.rightOperand &&
							individual1Join.preferredOperator == offspring1Join.preferredOperator &&
							individual2Join.leftOperand == offspring2Join.leftOperand &&
							individual2Join.rightOperand == offspring2Join.rightOperand &&
							individual2Join.preferredOperator == offspring2Join.preferredOperator) {
						admissibleCopy = true;
					} else if (individual1Join.leftOperand == offspring2Join.leftOperand &&
							individual1Join.rightOperand == offspring2Join.rightOperand &&
							individual1Join.preferredOperator == offspring2Join.preferredOperator &&
							individual2Join.leftOperand == offspring1Join.leftOperand &&
							individual2Join.rightOperand == offspring1Join.rightOperand &&
							individual2Join.preferredOperator == offspring1Join.preferredOperator) {
						admissibleCopy = true;
					}
					assertTrue(admissibleCopy);
				}
			}
		}
		// Mutation
		{
			// Make sure: if mutation probability is zero then the original individual is returned
			for (int i=0; i<50; ++i) {
				int nrTables = 1 + random.nextInt(10);
				Query query = QueryFactory.produceSteinbrunn(JoinGraphType.STAR, nrTables, JoinType.MN);
				Individual original = new Individual(query, planSpace, costModel);
				Individual mutation = original.mutated(0);
				for (int joinCtr=0; joinCtr<nrTables-1; ++joinCtr) {
					JoinPair originalPair = original.genes[joinCtr];
					JoinPair mutatedPair = mutation.genes[joinCtr];
					assertTrue(equivalentJoinPairs(originalPair, mutatedPair));
				}
			}
		}
		// Evaluation
		{
			for (int i=0; i<50; ++i) {
				Query query = QueryFactory.produceSteinbrunn(JoinGraphType.STAR, 5, JoinType.MN);
				JoinPair[] genes = new JoinPair[4];
				genes[0] = new JoinPair(0, 1, 0);
				genes[1] = new JoinPair(0, 1, 0);
				genes[2] = new JoinPair(0, 1, 0);
				genes[3] = new JoinPair(0, 1, 0);
				Individual individual = new Individual(query, planSpace, costModel, genes);
				individual.grow();
				assertEquals("(((((0)(1))(2))(3))(4))", individual.plan.orderToString());
				TestUtil.validatePlan(individual.plan, planSpace, costModel, true);
				assertArrayEquals(individual.plan.getCostValuesCopy(), individual.cost, EPSILON);
			}
		}
	}

}
