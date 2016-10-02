package optimizer.parallelized;

import java.io.Serializable;

/**
 * Generic result of invoking a worker node.
 * 
 * @author immanueltrummer
 *
 */
public class SlaveResult implements Serializable {
	/**
	 * Used to verify the class version.
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * The amount of main memory consumed while solving the input task.
	 * Mostly the amount of main memory is measured in terms of the
	 * number of relations that are treated.
	 */
	public final long mainMemoryConsumption;
	/**
	 * The number of milliseconds consumed for executing the input task
	 * on the slave node.
	 */
	public final long slaveTaskMillis;
	/**
	 * Whether a timeout occurred while executing the input task.
	 */
	public final boolean timeout;
	/**
	 * Whether a memory out occurred while executing the input task.
	 */
	public final boolean memoryOut;
	/**
	 * If any exception was thrown then this String contains the name.
	 */
	public final String errors;
	
	public SlaveResult(long mainMemoryConsumption, long slaveTaskMillis, 
			boolean timeout, boolean memoryOut, String errors) {
		this.mainMemoryConsumption = mainMemoryConsumption;
		this.slaveTaskMillis = slaveTaskMillis;
		this.timeout = timeout;
		this.memoryOut = memoryOut;
		this.errors = errors;
	}
}
