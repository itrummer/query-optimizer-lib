package plans.spaces;

import common.RandomNumbers;
import plans.Plan;
import plans.operators.JoinOperator;
import plans.operators.ScanOperator;
import relations.Relation;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Represents the plan space to consider by the optimizer.
 * The plan space determines which scan and join operators are applicable to given
 * operands.
 * 
 * @author immanueltrummer
 *
 */
public abstract class PlanSpace implements Serializable {
	/**
	 * Used to check the class version.
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * Default scan operator to use when optimizing join order without varying operators.
	 * This operator should be be selected to be applicable to all possible relations and produce
	 * output that is suitable as input for the default join operator.
	 */
	public ScanOperator defaultScanOperator;
	/**
	 * Default join operator to use when optimizing join order without varying operators.
	 * This operator should be applicable to input data that is produced by the default scan
	 * operator or by the default join operator itself.
	 */
	public JoinOperator defaultJoinOperator;
	/**
	 * The list of all scan operators. The list is generated during object construction.
	 */
	public List<ScanOperator> consideredScanOps;
	/**
	 * The list of all join operators. The list is generated during object construction.
	 */
	public List<JoinOperator> consideredJoinOps;
	/**
	 * Returns true if the given scan operator is applicable for the given relation.
	 * 
	 * @param scanOperator	the scan operator to test
	 * @param relation		the relation to which the scan should be applied
	 * @return				Boolean indicating whether the operator can be applied
	 */
	public abstract boolean scanOperatorApplicable(ScanOperator scanOperator, Relation relation);
	/**
	 * Returns true if the given scan operator is applicable to join the two given inputs.
	 * 
	 * @param joinOperator	the join operator to test
	 * @param leftPlan		the plan generating the left (outer) join input
	 * @param rightPlan		the plan generating the right (inner) join input
	 * @return				Boolean indicating whether the operator can be applied
	 */
	public abstract boolean joinOperatorApplicable(
			JoinOperator joinOperator, Plan leftPlan, Plan rightPlan);
	/**
	 * Returns true if the given scan operator produces output that is suitable as input
	 * for the given join operator.
	 * 
	 * @param scanOperator		scan operator producing output
	 * @param nextJoinOperator	join operator taking input
	 * @return					Boolean indicating whether scan output suitable as join input
	 */
	public abstract boolean scanOutputCompatible(ScanOperator scanOperator, JoinOperator nextJoinOperator);
	/**
	 * Returns true if the given join operator produces output that is suitable as input
	 * for given next join operator.
	 * 
	 * @param joinOperator		join operator producing output
	 * @param nextJoinOperator	join operator taking input
	 * @return					Boolean indicating whether current join output suitable as input
	 * 							for the next join operator
	 */
	public abstract boolean joinOutputComptatible(JoinOperator joinOperator, JoinOperator nextJoinOperator);
	/**
	 * Returns a list of applicable scan operators.
	 * 
	 * @param rel	the relation to scan
	 * @return		a list of applicable operators
	 */
	public List<ScanOperator> scanOperators(Relation rel) {
		List<ScanOperator> applicableOperators = new LinkedList<ScanOperator>();
		for (ScanOperator scanOperator : consideredScanOps) {
			if (scanOperatorApplicable(scanOperator, rel)) {
				applicableOperators.add(scanOperator);
			}
		}
		return applicableOperators;
	}
	/**
	 * Returns a list of applicable scan operators in random order. 
	 * 
	 * @param rel	the relation to scan
	 * @return		a list of applicable operators in random order
	 */
	public List<ScanOperator> scanOperatorsShuffled(Relation rel) {
		List<ScanOperator> applicableOperators = scanOperators(rel);
		Collections.shuffle(applicableOperators);
		return applicableOperators;
	}
	/**
	 * Returns a list of applicable join operators.
	 * 
	 * @param leftPlan	plan producing left (outer) join input
	 * @param rightPlan	plan producing right (inner) join input
	 * @return			a list of applicable join operators
	 */
	public List<JoinOperator> joinOperators(Plan leftPlan, Plan rightPlan) {
		List<JoinOperator> applicableJoinOperators = new LinkedList<JoinOperator>();
		for (JoinOperator joinOperator : consideredJoinOps) {
			if (joinOperatorApplicable(joinOperator, leftPlan, rightPlan)) {
				applicableJoinOperators.add(joinOperator);
			}
		}
		return applicableJoinOperators;
	}
	/**
	 * Returns a list of applicable join operators in random order.
	 * 
	 * @param leftPlan	plan producing left (outer) join input
	 * @param rightPlan	plan producing right (inner) join input
	 * @return			a list of applicable join operators in random order
	 */
	public List<JoinOperator> joinOperatorsShuffled(Plan leftPlan, Plan rightPlan) {
		List<JoinOperator> applicableJoinOperators = joinOperators(leftPlan, rightPlan);
		Collections.shuffle(applicableJoinOperators);
		return applicableJoinOperators;
	}
	/**
	 * Returns a random operator from a list of operators.
	 * 
	 * @param operators	list of scan or join operators
	 * @return			one randomly selected operator from the list
	 */
	<T> T randomOperator(List<T> operators) {
		int nrOperators = operators.size();
		int randomIndex = RandomNumbers.random.nextInt(nrOperators);
		return (operators.get(randomIndex));
	}
	/**
	 * Returns one randomly selected applicable scan operator.
	 * 
	 * @param rel	the relation that needs to be scanned by the operator
	 * @return		a random operator that can scan the relation
	 */
	public ScanOperator randomScanOperator(Relation rel) {
		List<ScanOperator> applicableOperators = scanOperators(rel);
		return randomOperator(applicableOperators);
	}
	/**
	 * Returns one randomly selected join operator among the applicable operators.
	 * 
	 * @param leftPlan	the plan producing the left (outer) join input
	 * @param rightPlan	the plan producing the right (inner) join input
	 * @return			a random join operator that can process the given inputs
	 */
	public JoinOperator randomJoinOperator(Plan leftPlan, Plan rightPlan) {
		List<JoinOperator> applicableOperators = joinOperators(leftPlan, rightPlan);
		return randomOperator(applicableOperators);
	}
	/**
	 * Returns random scan operator among the scan operators that can scan the given relation and 
	 * produce output that is suitable as input for the given next join operator.
	 * 
	 * @param rel				the relation that needs to be scanned
	 * @param nextJoinOperator	the join operator that needs to be fed with input from scan
	 * @return					a random scan operator satisfying all constraints
	 */
	public ScanOperator constrainedScanOperator(Relation rel, JoinOperator nextJoinOperator) {
		if (nextJoinOperator == null) {
			return randomScanOperator(rel);
		} else {
			List<ScanOperator> scanOperators = scanOperatorsShuffled(rel);
			Iterator<ScanOperator> scanOperatorsIter = scanOperators.iterator();
			while (scanOperatorsIter.hasNext()) {
				ScanOperator scanOperator = scanOperatorsIter.next();
				if (!scanOutputCompatible(scanOperator, nextJoinOperator)) {
					scanOperatorsIter.remove();
				}
			}
			return randomOperator(scanOperators);
		}
	}
	/**
	 * Returns random join operator among the join operators that can process the two given inputs
	 * and produce output that is suitable as input for the given next join operator.
	 * 
	 * @param leftPlan			plan producing left (outer) join input
	 * @param rightPlan			plan producing right (inner) join input
	 * @param nextJoinOperator	the join output must be suitable as input for that operator
	 * @return					a random join operator satisfying all constraints
	 */
	public JoinOperator constrainedJoinOperator(Plan leftPlan, Plan rightPlan, JoinOperator nextJoinOperator) {
		if (nextJoinOperator == null) {
			return randomJoinOperator(leftPlan, rightPlan);
		} else {
			List<JoinOperator> joinOperators = joinOperatorsShuffled(leftPlan, rightPlan);
			Iterator<JoinOperator> joinOperatorsIter = joinOperators.iterator();
			while (joinOperatorsIter.hasNext()) {
				JoinOperator joinOperator = joinOperatorsIter.next();
				if (!joinOutputComptatible(joinOperator, nextJoinOperator)) {
					joinOperatorsIter.remove();
				}
			}
			return randomOperator(joinOperators);
		}
	}
}
