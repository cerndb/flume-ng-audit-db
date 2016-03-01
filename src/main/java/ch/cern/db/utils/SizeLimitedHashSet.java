package ch.cern.db.utils;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * It acts as a queue, removing last element inserted when Set reach maximun size.
 * 
 * @author daniellanzagarcia
 *
 * @param <E>
 */
@SuppressWarnings("serial")
public class SizeLimitedHashSet<E> extends HashSet<E> {
	
	private LinkedList<E> list;
	
	private int maximunSize = -1; //No limit

	public SizeLimitedHashSet() {
		this(-1);
	}
	
	public SizeLimitedHashSet(int maximunSize) {
		super();
		
		this.maximunSize = maximunSize;
		
		list = new LinkedList<E>();
	}
	
	@Override
	public boolean add(E e) {
		if(super.add(e)){
			if(maximunSize > -1 && list.size() >= maximunSize)
				remove(list.removeFirst());
			
			return list.add(e);
		}else{
			return false;
		}
	}
	
	@Override
	public boolean addAll(Collection<? extends E> c) {
		boolean modified = false;
        for (E e : c)
            if (add(e))
                modified = true;
        return modified;
	}
	
	@Override
	public boolean remove(Object e) {
		if(super.remove(e))
			return list.remove(e);
		else
			return false;
	}
	
	@Override
	public boolean removeAll(Collection<?> e) {
		if(super.removeAll(e))
			return list.removeAll(e);
		else
			return false;
	}
	
	@Override
	public void clear() {
		super.clear();
		list.clear();
	}
}
