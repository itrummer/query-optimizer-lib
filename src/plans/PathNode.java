package plans;

/**
 * Used to describes path from original plan root to some partial plan.
 * 
 * @author immanueltrummer
 *
 */
public class PathNode {
	/**
	 * The plan represented by the path node.
	 */
	public Plan plan;
	/**
	 * The parent node on the path.
	 */
	public PathNode parent;
	/**
	 * Whether this path node was the left child of its parent.
	 */
	public boolean isLeftChild;
	
	public PathNode(Plan plan, PathNode parent, boolean isLeftChild) {
		this.plan = plan;
		this.parent = parent;
		this.isLeftChild = isLeftChild;
	}
	/**
	 * Checks whether this node represents the root (if parent is null)
	 * 
	 * @return	Boolean indicating whether this node is associated with the plan root
	 */
	public boolean isRoot() {
		return parent == null;
	}
}