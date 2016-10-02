package plans;

/**
 * The join order space to search by the parallelized optimizer, either linear or bushy plans.
 * - LINEAR: linear (also called left-deep) plans allow only single table as right join operand
 * - BUSHY: bushy plans allow arbitrary join operands and generalize linear plans
 * 
 * @author immanueltrummer
 *
 */
public enum JoinOrderSpace {
	LINEAR, BUSHY
}
