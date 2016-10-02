package plans;

import java.io.Serializable;
import java.util.List;

/**
 * Contains a set of (near-)Pareto-optimal plans.
 * 
 * @author immanueltrummer
 *
 */
public class ParetoPlanSet implements Serializable {
	/**
	 * Used to verify class version.
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * A list of Pareto-optimal plans.
	 */
	public final List<Plan> plans;
	
	public ParetoPlanSet(List<Plan> plans) {
		this.plans = plans;
	}
}
