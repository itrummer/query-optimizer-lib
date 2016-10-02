package cost.local;

import cost.SingleCostModel;
import plans.JoinPlan;
import plans.Plan;
import plans.ScanPlan;
import plans.operators.JoinOperator;
import plans.operators.ScanOperator;
import plans.operators.cluster.ClusterScan;
import plans.operators.local.BNLjoin;
import plans.operators.local.HashJoin;
import plans.operators.local.LocalScan;
import plans.operators.local.SortMergeJoin;
import util.MathUtil;

/**
 * Estimates the execution time of a query plan. The cost formulas are based on the VLDB'97
 * paper by Steinbrunn et al. "Heuristic and randomized optimization for the join ordering
 * problem".
 * 
 * @author immanueltrummer
 *
 */
public class TimeCostModel extends SingleCostModel {
	/**
	 * Used to verify the class version.
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * This constructor allows to specify the index at which the corresponding cost values
	 * are stored within the cost vectors.
	 * 
	 * @param metricIndex	index of this cost metric within the cost vectors
	 */
	public TimeCostModel(int metricIndex) {
		super(metricIndex);
	}
	
	@Override
	protected void updateScanRoot(ScanPlan plan) {
		ScanOperator scanOperator = plan.scanOperator;
		if (scanOperator instanceof LocalScan) {
			// Attention: this benchmark does not consider single-table predicates:
			// only therefore input and output size of the scan is identical.
			// plan.cost[metricIndex] = plan.outputPages;
			
			// Scan cost will be counted at the first join
			plan.setCostValue(metricIndex, 0);
		} else {
			assert(scanOperator instanceof ClusterScan);
			assert(false);
		}
	}
	/**
	 * Calculates the fraction of a relation whose hash table fits into memory.
	 * 
	 * @param leftPages	the number of buffer pages consumed by the left oeprand 
	 * @param buffer	the amount of buffer space reserved for this join operator
	 * @return			the fraction of the hash table of the left relation that fits into main memory 
	 */
	double calculateTableFraction(double leftPages, double buffer) {
		assert(buffer > 0);
		if (Double.isInfinite(leftPages)) {
			return 0;
		} else {
			double q = (buffer - Math.ceil((1.4*leftPages-buffer)/(buffer-1))) / leftPages;
			q = Math.max(q, 0);
			q = Math.min(q, 1);
			return q;			
		}
	}
	@Override
	protected void updateJoinRoot(JoinPlan plan) {
		// store important fields in local variables for quick access
		JoinOperator genericJoin = plan.getJoinOperator();
		Plan leftPlan = plan.getLeftPlan();
		Plan rightPlan = plan.getRightPlan();
		double leftPages = leftPlan.outputPages;
		double rightPages = rightPlan.outputPages;
		double leftGenerationCost = leftPlan.getCostValue(metricIndex);
		double rightGenerationCost = rightPlan.getCostValue(metricIndex);
		// calculate cost for reading and generating both inputs
		double inputCost = -1;
		if (genericJoin instanceof BNLjoin) {
			BNLjoin join = (BNLjoin)genericJoin;
			// In case of pipelining, the left operand is not materialized hence no reading cost
			double leftReadCost = leftPlan.materializes ? leftPages : 0;
			double leftCost = leftGenerationCost + leftReadCost;
			// Either right input is materialized or we have to re-execute it for each outer loop iteration
			double nrOuterIterations = Math.ceil(leftPages / join.buffer);
			double rightCost = rightPlan.materializes ? 
					nrOuterIterations * rightPages + rightGenerationCost :
						nrOuterIterations * rightGenerationCost;
			inputCost = leftCost + rightCost;
			
		} else if (genericJoin instanceof HashJoin) {
			assert(leftPlan.materializes);
			assert(rightPlan.materializes);
			HashJoin join = (HashJoin)genericJoin;
			// q is ratio of left table whose hash table fits into memory
			double buffer = join.buffer;
			double q = calculateTableFraction(leftPages, buffer);
			// Must treat the special case that input size or generation cost have
			// become too large to be represented accurately. In that case, the cost
			// is infinite.
			inputCost = Double.isInfinite(leftPages) || Double.isInfinite(rightPages) ||
					Double.isInfinite(leftGenerationCost) || 
					Double.isInfinite(rightGenerationCost) ?
					Double.POSITIVE_INFINITY :
					leftPages + rightPages + 2 * (leftPages + rightPages) * (1-q) 
					+ leftGenerationCost + rightGenerationCost;
			assert(q>=0 && q<=1 && inputCost >= 0) :
				"inputCost: " + inputCost +
				"; leftPages: " + leftPages + "; buffer: " + buffer + "; q: " + q +
				"; leftPlan: " + leftPlan + "; leftRel: " + leftPlan.resultRel +
				"; rightPages: " + rightPages + "; leftGenerationCost: " + leftGenerationCost +
				"; rightGenerationCost: " + rightGenerationCost;
			
		} else if (genericJoin instanceof SortMergeJoin){
			assert(leftPlan.materializes);
			assert(rightPlan.materializes);
			SortMergeJoin join = (SortMergeJoin)genericJoin;
			double buffer = join.buffer;
			// We assume that sorting is always required
			double leftCost = leftPages + leftPages * MathUtil.logOfBase(buffer, leftPages);
			double rightCost = rightPages + rightPages * MathUtil.logOfBase(buffer, rightPages);
			inputCost = leftCost + rightCost + leftGenerationCost + rightGenerationCost;
			
		} else {
			assert (false);
		}
		assert(inputCost >= 0) : "inputCost: " + inputCost + "; plan with old cost: " + plan;
		// calculate cost for writing output
		double outputCost = plan.materializes ? plan.outputPages : 0;
		plan.setCostValue(metricIndex, inputCost + outputCost);
	}
	
	@Override
	public String toString() {
		return "Time";
	}
}
