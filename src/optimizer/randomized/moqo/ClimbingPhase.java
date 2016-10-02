package optimizer.randomized.moqo;

import plans.Plan;
import util.LocalSearchUtil;

/**
 * Implements hill climbing to improve query plans. Either tries all possible moves to find
 * a dominating plan or tries a fixed number of random moves before giving up.
 * 
 * @author immanueltrummer
 *
 */
public class ClimbingPhase extends Phase {
	/**
	 * Configures whether all possible moves are considered from each search node or only
	 * a randomly selected subset.
	 */
	final boolean tryAllMoves;
	/**
	 * The default number of moves to try before giving up on improving a plan is scaled by
	 * this factor.
	 */
	final int nrTriesScale;

	public ClimbingPhase(boolean tryAllMoves, int nrTriesScale) {
		this.tryAllMoves = tryAllMoves;
		this.nrTriesScale = nrTriesScale;
	}
	/**
	 * If no current plan is given then a random plan is generated, otherwise either all moves
	 * or a fixed number of random moves are tried out from the current plan. We return the new
	 * plan if an improvement was reached or a null pointer if no improvement was possible.
	 */
	@Override
	public Plan nextPlan(Plan currentPlan) {
		if (currentPlan == null) {
			// If no current plan then regenerate one
			Plan randomPlan = LocalSearchUtil.randomBushyPlan(query, planSpace);
			costModel.updateAll(randomPlan);
			return randomPlan;
		} else {
			// If we have current plan then try to improve it
			if (tryAllMoves) {
				// Exhaustively try all ways of mutating the plan
				return LocalSearchUtil.ParetoClimb(
						query, currentPlan, planSpace, costModel, consideredMetrics);
				/*
				return LocalSearchUtil.improvedPlan(
						query, currentPlan, consideredMetrics, 
						planSpace, costModel, null);
				*/
			} else {
				// Try a certain number of random moves to improve the plan
				double[] curCost = currentPlan.getCostValuesCopy();
				int nrTries = (query.nrTables - 1) * nrTriesScale;
				for (int tryCtr=0; tryCtr<nrTries; ++tryCtr) {
					Plan randomPlan = LocalSearchUtil.randomMove(
							query, currentPlan, planSpace, costModel);
					double[] newCost = randomPlan.getCostValuesCopy();
					if (LocalSearchUtil.acceptMove(curCost, newCost,
							consideredMetrics, false, -1)) {
						return randomPlan;
					}
				}
				// Current plan assumed to be local optimum - 
				// force re-initialization by returning null pointer.
				return null;
			}
		}
	}
	@Override
	public String toString() {
		return "Climb";
	}
}
