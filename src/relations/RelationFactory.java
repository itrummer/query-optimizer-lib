package relations;

import java.util.BitSet;
import queries.Query;

/**
 * Produces relation objects (i.e., calculates among others the cardinality, byte size etc.
 * of the new relations).
 * 
 * @author immanueltrummer
 *
 */
public class RelationFactory {
	/**
	 * Creates a relation representing only a single table.
	 * 
	 * @param query			the query to which the table belongs
	 * @param tableIndex	the index of the table that shall be represented
	 * @return				a relation representing the specified table
	 */
	public static Relation createSingleTableRel(Query query, int tableIndex) {
		// create table set
		BitSet tableSet = new BitSet();
		tableSet.set(tableIndex);
		// get cardinality
		double cardinality = query.tableCardinalities[tableIndex];
		// create new relation
		return new Relation(tableSet, cardinality);
	}
	/**
	 * Creates a new relation by joining two existing relations.
	 * 
	 * @param query	the query to which all joined tables belong
	 * @param rel1	the first relation to be joined
	 * @param rel2	the second relation to be joined
	 * @return		a new relation representing the join of the two given relations
	 */
	public static Relation createJoinRel(Query query, Relation rel1, Relation rel2) {
		// verify that the joined relations have no tables in common
		assert(!rel1.tableSet.intersects(rel2.tableSet)) : "rel1: " + rel1.tableSet + "; rel2: " + rel2.tableSet;
		// create result table set
		BitSet resultSet = new BitSet();
		resultSet.or(rel1.tableSet);
		resultSet.or(rel2.tableSet);
		// calculate result relation cardinality
		double resultCardinality = 1.0;
		resultCardinality *= rel1.cardinality;
		resultCardinality *= rel2.cardinality;
		// take into account applicable join predicates
		BitSet tableSet1 = rel1.tableSet;
		BitSet tableSet2 = rel2.tableSet;
		for (int tableIndex1 = tableSet1.nextSetBit(0); tableIndex1 >= 0; tableIndex1 = tableSet1.nextSetBit(tableIndex1+1)) {
			for (int tableIndex2 = tableSet2.nextSetBit(0); tableIndex2 >= 0; tableIndex2 = tableSet2.nextSetBit(tableIndex2+1)) {
				resultCardinality *= query.selectivities[tableIndex1][tableIndex2];
			 }
		 }
		// create and return new relation
		return new Relation(resultSet, resultCardinality);
	}
}
