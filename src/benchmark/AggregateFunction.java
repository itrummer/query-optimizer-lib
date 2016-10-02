package benchmark;

/**
 * Describes the way in which feature values for the same query size and time period are
 * aggregated:
 * MEAN		arithmetic mean
 * MEDIAN	the median
 * NR_WINS	number of queries for which one algorithm has lower or same value as all others
 * 
 * @author immanueltrummer
 *
 */
public enum AggregateFunction {
	MEAN, MEDIAN, NR_WINS
}
