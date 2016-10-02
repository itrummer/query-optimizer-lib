package plans;

import java.util.Arrays;

import plans.Plan;
import plans.operators.ScanOperator;
import queries.Query;
import relations.Relation;
import relations.RelationFactory;

/**
 * Represents a query plan scanning a single relation. 
 * 
 * @author immanueltrummer
 *
 */
public class ScanPlan extends Plan {
	/**
	 * Used to verify the class version.
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * The index of the scanned table.
	 */
	public final int tableIndex;
	/**
	 * The operator implementation used for the scan.
	 */
	public final ScanOperator scanOperator;
	/**
	 * This constructor stores a given result relation.
	 * 
	 * @param rel			the previously generated relation representing the result of the scan
	 * @param scanOperator	the operator implementation used for the scan
	 */
	public ScanPlan(Relation rel, ScanOperator scanOperator) {
		super(rel, true, 1);
		this.tableIndex = rel.tableSet.nextSetBit(0);
		this.scanOperator = scanOperator;
	}
	/**
	 * This constructor generates and stores the result relation.
	 * 
	 * @param query			the query being optimized
	 * @param tableIndex	the index of the scanned table
	 * @param scanOperator	the operator implementation used for the scan
	 */
	public ScanPlan(Query query, int tableIndex, ScanOperator scanOperator) {
		super(RelationFactory.createSingleTableRel(query, tableIndex), true, 1);
		this.tableIndex = resultRel.tableSet.nextSetBit(0);
		this.scanOperator = scanOperator;
	}
	/**
	 * This constructor stores a null pointer for the result relation.
	 * 
	 * @param outputRows	the cardinality of the scanned relation
	 * @param outputPages	the number of pages consumed by the scanned relation
	 * @param tableIndex	the index of the scanned relation
	 * @param scanOperator	the operator implementation used for the scan
	 */
	public ScanPlan(double outputRows, double outputPages,  
			int tableIndex, ScanOperator scanOperator) {
		super(outputRows, outputPages, true, 1);
		this.tableIndex = tableIndex;
		this.scanOperator = scanOperator;
	}
	@Override
	public String toString() {
		return "(" + scanOperator.toString() + Arrays.toString(cost) + tableIndex + ")";
	}
	@Override
	public String orderToString() {
		return "(" + tableIndex + ")";
	}
	@Override
	public void makeImmutable() {
		immutable = true;
	}
	@Override
	public ScanPlan deepMutableCopy() {
		ScanPlan copy;
		if (resultRel == null) {
			copy = new ScanPlan(outputRows, outputPages, tableIndex, (ScanOperator)scanOperator.deepCopy());
		} else {
			copy = new ScanPlan(resultRel, (ScanOperator)scanOperator.deepCopy());
		}
		copy.setCostValues(getCostValuesCopy());
		return copy;
	}
}
