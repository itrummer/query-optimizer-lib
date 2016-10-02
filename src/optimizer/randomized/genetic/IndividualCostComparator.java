package optimizer.randomized.genetic;

import java.util.Comparator;

/**
 * This class is used to compare individuals according to a specific cost metric
 * whose index is passed as constructor argument.
 * 
 * @author immanueltrummer
 *
 */
class IndividualCostComparator implements Comparator<Individual> {
	/**
	 * The index of the cost metric according to which we compare individuals.
	 */
	final int metricIndex;
	
	public IndividualCostComparator(int metricIndex) {
		this.metricIndex = metricIndex;
	}
	@Override
	public int compare(Individual o1, Individual o2) {
		return Double.compare(o1.cost[metricIndex], o2.cost[metricIndex]);
	}
}