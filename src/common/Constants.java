package common;

/**
 * This class contains several constants that are used all over the code base, in
 * particular configuration settings for performance benchmarks.
 * 
 * @author immanueltrummer
 *
 */
public class Constants {
	/**
	 * Determines for benchmarks with anytime optimizers how many milliseconds each
	 * optimization algorithm can run.
	 */
	//public static long TIMEOUT_MILLIS = 3000;
	//public static long TIMEOUT_MILLIS = 30000;
	public static long TIMEOUT_MILLIS = 1000;
	/**
	 * Optimization time is divided into that many intervals and performance statistics
	 * may be generated for each separate optimization time interval. This allows to
	 * recognize anytime algorithms that produce a decent solution very quickly as well
	 * as anytime optimizers that take a long time to produce a very good solution.
	 */
	//public final static int NR_TIME_PERIODS = 30;
	// TODO
	//public final static int NR_TIME_PERIODS = 30;
	public final static int NR_TIME_PERIODS = 1;
	/**
	 * One optimization time period has so many milliseconds.
	 */
	public static long TIME_PERIOD_MILLIS = TIMEOUT_MILLIS/NR_TIME_PERIODS;
	/**
	 * So many cost metrics for query plan execution cost are maximally considered.
	 */
	public static int NR_COST_METRICS = 1;
	/**
	 * Determines for benchmarking of parallelized optimizers which degrees of parallelism
	 * are tried. Will be initialized from the command line arguments.
	 */
	public static int[] DEGREES_OF_PARALLELISM = null;
	//public final static int NR_COST_METRICS = 3;			// Number of cost metrics on query plans
	//public final static int NR_COST_METRICS = 2;			// Number of cost metrics on query plans
	// public final static double BYTES_PER_TUPLR = 50;		// average number bytes per relation tuple
	/**
	 * Average number of bytes per relation tuple.
	 */
	public static double BYTES_PER_TUPLE = 100;
	/**
	 * One buffer page contains that many bytes.
	 */
	// TODO
	public static double BYTES_PER_PAGE = 1024;
	/**
	 * Configuration parameter for MapReduce cost model.
	 */
	public final static double[] IO_SORT_MB_VALUES = new double[] {
		100*1E6, 150*1E6, 200*1E6, 250*1E6, 300*1E6, 350*1E6, 400*1E6, 450*1E6, 500*1E6, 550*1E6};
	/**
	 * Configuration parameter for MapReduce cost model.
	 */
	public final static double[] IO_SORT_FACTOR = 
			new double[] {100, 120, 140, 160, 180, 200, 220, 240, 260, 280, 300};
	/**
	 * If this flag is set to true then additional checks are performed and additional
	 * logging output is generated. Should only be used during debugging but not during
	 * performance benchmarks as the added checks can be expensive.
	 */
	public final static boolean SAFE_MODE = false;
}
