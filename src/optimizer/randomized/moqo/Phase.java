package optimizer.randomized.moqo;

import cost.MultiCostModel;
import plans.Plan;
import plans.spaces.PlanSpace;
import queries.Query;

/**
 * Represents a phase within a randomized search algorithm. A randomized search algorithm
 * might have several phases that are executed consecutively. Each phases is initialized once
 * per query and invoked until a public flag indicates that the phase is terminated. While
 * executing, each phase generates a series of query plans and obtains the last generated
 * plan as input parameter.
 * 
 * @author immanueltrummer
 *
 */
public abstract class Phase {
	protected Query query;
	protected boolean[] consideredMetrics;
	protected PlanSpace planSpace;
	protected MultiCostModel costModel;
	
	public void init(Query query, boolean[] consideredMetrics, 
			PlanSpace planSpace, MultiCostModel costModel) {
		this.query = query;
		this.consideredMetrics = consideredMetrics;
		this.planSpace = planSpace;
		this.costModel = costModel;
	}
	public abstract Plan nextPlan(Plan currentPlan);
}
