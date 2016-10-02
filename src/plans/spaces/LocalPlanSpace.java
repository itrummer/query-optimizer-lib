package plans.spaces;

import java.util.LinkedList;

import plans.Plan;
import plans.operators.JoinOperator;
import plans.operators.ScanOperator;
import plans.operators.local.BNLjoin;
import plans.operators.local.HashJoin;
import plans.operators.local.LocalScan;
import plans.operators.local.SortMergeJoin;
import relations.Relation;

/**
 * This plan space describes a single-node execution scenario in which different configurations
 * of the standard join operators (block-nested loop, hash join, sort-merge join) are applicable.
 * 
 * @author immanueltrummer
 *
 */
public class LocalPlanSpace extends PlanSpace {
	/**
	 * Used to verify the class version.
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * The non-parameterized constructor generates the standard variant of this
	 * plan space which is appropriate for multi-objective query optimization
	 * scenarios.
	 */
	public LocalPlanSpace() {
		this(LocalSpaceVariant.MOQO);
	}
	/**
	 * The parameterized variant of the constructor allows to explicitly choose
	 * the plan space variant.
	 * 
	 * @param variant	the plan space variant determining the available join operators
	 */
	public LocalPlanSpace(LocalSpaceVariant variant) {
		// initialize list of scan and join operators
		consideredScanOps = new LinkedList<ScanOperator>();
		consideredJoinOps = new LinkedList<JoinOperator>();
		// scan operators
		consideredScanOps.add(new LocalScan());
		// join operators
		switch (variant) {
		case MOQO:
		{
			for (Boolean materializes : new Boolean[]{false, true}) {
				for (Integer buffer : new Integer[]{10, 100, 1000, 10000, 100000}) {
					consideredJoinOps.add(new BNLjoin(buffer, materializes));
					consideredJoinOps.add(new HashJoin(buffer, materializes));
					consideredJoinOps.add(new SortMergeJoin(buffer, materializes));
				}
			}
		}
		break;
		case SOQO:
		{
			int buffer = 10000;
			boolean materializes = true;
			consideredJoinOps.add(new BNLjoin(buffer, materializes));
			consideredJoinOps.add(new HashJoin(buffer, materializes));
			consideredJoinOps.add(new SortMergeJoin(buffer, materializes));
		}
		break;
		case SIMPLE:
		{
			consideredJoinOps.add(new BNLjoin(10000, true));
		}
		break;
		default:
			assert(false) : "Unknown local plan space variant!";
		}
		// default scan and join operators
		defaultScanOperator = new LocalScan();
		//defaultJoinOperator = new BNLjoin(100, true);
		defaultJoinOperator = new BNLjoin(10000, true);
	}
	/**
	 * The local scan operator is applicable to each relation.
	 */
	@Override
	public boolean scanOperatorApplicable(ScanOperator scanOperator, Relation relation) {
		return true;	// changes once we consider indices
	}
	/**
	 * Only the block-nested loop operator allows pipelining while the others require
	 * materialized input (we use the same assumptions as Steinbrunn et al., VLDB 1997).
	 */
	@Override
	public boolean joinOperatorApplicable(JoinOperator joinOperator, Plan leftPlan, Plan rightPlan) {
		if (joinOperator instanceof BNLjoin) {
			return true;
		} else if (joinOperator instanceof HashJoin) {
			return leftPlan.materializes && rightPlan.materializes;
		} else {
			assert(joinOperator instanceof SortMergeJoin);
			return leftPlan.materializes && rightPlan.materializes;
		}
	}
	/**
	 * The output of the local scan operator is suitable as input to any join operator.
	 */
	@Override
	public boolean scanOutputCompatible(ScanOperator scanOperator,
			JoinOperator nextJoinOperator) {
		return true;
	}
	/**
	 * Hash join and merge join require materialized relations as output while the
	 * block-nested-loop join can be used for pipelining.
	 */
	@Override
	public boolean joinOutputComptatible(JoinOperator joinOperator,
			JoinOperator nextJoinOperator) {
		if (nextJoinOperator instanceof HashJoin || nextJoinOperator instanceof SortMergeJoin) {
			return joinOperator.materializeResult;
		} else {
			return true;
		}
	}
	@Override
	public String toString() {
		return "Local Processing Plan Space";
	}
}
