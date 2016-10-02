package plans.spaces;

/**
 * Different variants of the local plan space are available that differ by the
 * set of considered operators:
 * - MOQO		features different versions of each join operator that use different amount of buffer space
 * - SOQO		features the three standard join operators but with a fixed amount of buffer space
 * - SIMPLE		features only one standard join operator
 * The default variant is MOQO.
 * 
 * @author immanueltrummer
 *
 */
public enum LocalSpaceVariant {
	MOQO, SOQO, SIMPLE
}
