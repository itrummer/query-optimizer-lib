package util;

import java.util.Collections;
import java.util.List;

import cost.MultiCostModel;
import plans.JoinPlan;
import plans.Plan;
import plans.operators.JoinOperator;
import plans.spaces.PlanSpace;
import queries.Query;

public class SamplingUtil {

	public static void performJoin(Query query, PlanSpace planSpace, 
			MultiCostModel costModel, List<Plan> partialPlans, double maxHeightCutoff) {
		Collections.shuffle(partialPlans);
		// Find out what is the highest plan among the partial plans
		int maxHeight = 0;
		for (Plan partialPlan : partialPlans) {
			maxHeight = Math.max(maxHeight, partialPlan.height);
		}
		// Select and remove left join operand
		Plan leftOperand = null;
		for (Plan partialPlan : partialPlans) {
			if (partialPlan.height >= maxHeight * maxHeightCutoff) {
				leftOperand = partialPlan;
				break;
			}
		}
		partialPlans.remove(leftOperand);
		// Select and remove right operand
		Plan rightOperand = partialPlans.get(0);
		partialPlans.remove(0);
		// Prepare and add join plan
		JoinOperator joinOperator = planSpace.randomJoinOperator(leftOperand, rightOperand);
		JoinPlan joinPlan = new JoinPlan(query, leftOperand, rightOperand, joinOperator);
		partialPlans.add(joinPlan);
		costModel.updateRoot(joinPlan);
	}
	
	public static Plan samplePlan(Query query, PlanSpace planSpace, MultiCostModel costModel, 
			List<Plan> partialPlans, double maxHeightCutoff) {
		while (partialPlans.size() > 1) {
			performJoin(query, planSpace, costModel, partialPlans, maxHeightCutoff);
		}
		return partialPlans.iterator().next();
	}

}
