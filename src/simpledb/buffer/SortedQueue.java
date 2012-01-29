package simpledb.buffer;

import java.util.Collection;
import java.util.Queue;
import java.util.TreeSet;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class SortedQueue<E> extends TreeSet<E> implements Collection<E>, Queue<E>
{

	@Override
	public boolean offer(E e)
	{
		throw new NotImplementedException();
	}

	@Override
	public E remove()
	{
		if (this.isEmpty()) throw new RuntimeException("empty set");
		E obj = this.first();
		this.remove(obj);
		return obj;
	}

	@Override
	public E poll()
	{
		if (this.isEmpty()) return null;
		E obj = this.first();
		this.remove(obj);
		return obj;
	}

	@Override
	public E element()
	{
		if (this.isEmpty()) throw new RuntimeException("empty set");
		return this.first();
	}

	@Override
	public E peek() {
		if (this.isEmpty()) return null;
		return this.first();
	}
	
	@Override
	public boolean add(E e)
	{
		if (e instanceof TimedBuffer) ((TimedBuffer)e).setSet((SortedQueue<TimedBuffer>)this);
		return super.add(e);
	}
}
