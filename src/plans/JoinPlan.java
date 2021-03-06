package plans;

import java.util.Arrays;

import plans.Plan;
import plans.operators.JoinOperator;
import queries.Query;
import relations.Relation;
import relations.RelationFactory;

/**
 * Represents a binary join of two intermediate results or base tables.
 * 
 * @author immanueltrummer
 *
 */
public class JoinPlan extends Plan {
	/**
	 * Used to verify the class version.
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * The left plan produces the outer operand for this join and the right plan produces
	 * the inner operand for this join.
	 */
	private Plan leftPlan, rightPlan;
	/**
	 * The join operator implementation used for the final join (i.e., the join at the root
	 * of this plan tree). 
	 */
	private JoinOperator joinOperator;
	/**
	 * This constructor stores a given relation as result relation.
	 * 
	 * @param leftRel		the relation that forms the outer operand of the final join
	 * @param rightRel		the relation that forms the inner operand of the final join
	 * @param resultRel		the relation resulting from executing this plan
	 * @param leftPlan		the plan used to produce the left relation
	 * @param rightPlan		the plan used to produce the right relation
	 * @param joinOperator	the join operator implementation used for the final join
	 */
	public JoinPlan(Relation leftRel, Relation rightRel, Relation resultRel, 
			Plan leftPlan, Plan rightPlan, JoinOperator joinOperator) {
		super(resultRel, joinOperator.materializeResult, 
				1 + Math.max(leftPlan.height, rightPlan.height));
		this.leftPlan = leftPlan;
		this.rightPlan = rightPlan;
		this.joinOperator = joinOperator;
	}
	/**
	 * This constructor generates and stores the result relation.
	 * 
	 * @param query			the query being optimized
	 * @param leftPlan		the plan producing the outer operand for the final join
	 * @param rightPlan		the plan producing the inner operand for the final join
	 * @param joinOperator	the join operator implementation used for the final join
	 */
	public JoinPlan(Query query, Plan leftPlan, Plan rightPlan, JoinOperator joinOperator) {
		super(RelationFactory.createJoinRel(query, leftPlan.resultRel, rightPlan.resultRel), 
				joinOperator.materializeResult, 1 + Math.max(leftPlan.height, rightPlan.height));
		this.leftPlan = leftPlan;
		this.rightPlan = rightPlan;
		this.joinOperator = joinOperator;
	}
	/**
	 * This constructor stores a null pointer as result relation.
	 * 
	 * @param outputRows	cardinality of output generated by this plan
	 * @param outputPages	number of disc pages generated by this plan
	 * @param leftPlan		the plan producing the outer operand of the final join
	 * @param rightPlan		the plan producing the inner operand of the final join
	 * @param joinOperator	the join operator implementation used for the final join
	 */
	public JoinPlan(double outputRows, double outputPages, 
			Plan leftPlan, Plan rightPlan, JoinOperator joinOperator) {
		super(outputRows, outputPages, joinOperator.materializeResult, 
				1 + Math.max(leftPlan.height, rightPlan.height));
		this.leftPlan = leftPlan;
		this.rightPlan = rightPlan;
		this.joinOperator = joinOperator;
	}
	@Override
	public String toString() {
		String output = "";
		output += "(";
		output += joinOperator.toString();
		output += Arrays.toString(cost);
		output += leftPlan.toString() + " ";
		output += rightPlan.toString() + ")";
		return output;
	}
	@Override
	public String orderToString() {
		return "(" + leftPlan.orderToString() + rightPlan.orderToString() + ")";
	}
	@Override
	public void makeImmutable() {
		leftPlan.makeImmutable();
		rightPlan.makeImmutable();
		immutable = true;
	}
	/**
	 * Sets left sub-plan for mutable plans
	 * 
	 * @param newLeftPlan	new left plan replacing current one
	 */
	public void setLeftPlan(Plan newLeftPlan) {
		assert(!immutable);
		leftPlan = newLeftPlan;
	}
	/**
	 * Sets right sub-plan for mutable plans
	 * 
	 * @param newRightPlan	new right plan replacing current one
	 */
	public void setRightPlan(Plan newRightPlan) {
		assert(!immutable);
		rightPlan = newRightPlan;
	}
	/**
	 * Sets join operator for mutable plans
	 * 
	 * @param newJoinOperator	new operator for final join
	 */
	public void setJoinOperator(JoinOperator newJoinOperator) {
		assert(!immutable);
		joinOperator = newJoinOperator;
	}
	/**
	 * Returns left sub-plan
	 * 
	 * @return	left sub-plan
	 */
	public Plan getLeftPlan() {
		return leftPlan;
	}
	/**
	 * Returns right sub-plan
	 * 
	 * @return	right sub-plan
	 */
	public Plan getRightPlan() {
		return rightPlan;
	}
	/**
	 * Returns the operator used for the final join
	 * 
	 * @return	the final join operator
	 */
	public JoinOperator getJoinOperator() {
		return joinOperator;
	}
	@Override
	public JoinPlan deepMutableCopy() {
		JoinPlan copy;
		if (resultRel == null) {
			copy = new JoinPlan(outputRows, outputPages, leftPlan.deepMutableCopy(), 
					rightPlan.deepMutableCopy(), (JoinOperator)joinOperator.deepCopy());
		} else {
			copy = new JoinPlan(leftPlan.resultRel, rightPlan.resultRel, resultRel, 
					leftPlan.deepMutableCopy(), rightPlan.deepMutableCopy(), 
					(JoinOperator)joinOperator.deepCopy());
		}
		copy.setCostValues(getCostValuesCopy());
		return copy;
	}
}
