package benchmark;

import java.util.Arrays;

import queries.Query;

/**
 * A test case used to compare different query optimization algorithms. A test case is
 * characterized by the query to optimize and by the set of considered cost metrics.
 * 
 * @author immanueltrummer
 *
 */
public class TestCase {
	/**
	 * the query to optimize
	 */
	public final Query query;
	/**
	 * array of Boolean flags indicating for each plan cost metric whether it is considered
	 */
	public final boolean[] consideredMetrics;
	/**
	 * Constructs a TestCase from the given query and set of plan cost metrics.
	 * 
	 * @param query
	 * @param consideredMetrics
	 */
	public TestCase(Query query, boolean[] consideredMetrics) {
		this.query = query;
		this.consideredMetrics = consideredMetrics;
	}
	/**
	 * Outputs considered cost metrics and the query to optimize.
	 */
	@Override
	public String toString() {
		return "consideredMetrics: " + Arrays.toString(consideredMetrics) + "; query: " + query.toString();
	}
}
