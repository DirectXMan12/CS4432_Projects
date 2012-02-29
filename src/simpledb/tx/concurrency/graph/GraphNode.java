package simpledb.tx.concurrency.graph;

public class GraphNode
{
	protected int _id;
	
	public GraphNode(int id)
	{
		_id = id;
	}
	
	@Override
	public String toString()
	{
		return "GraphNode@"+_id;
	}

}
