package ch.cern.db.utils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * It acts as a queue, removing last element inserted when Set reach maximum size.
 * 
 * @author daniellanzagarcia
 *
 * @param <E>
 */
@SuppressWarnings("serial")
public class SizeLimitedHashSet<E> extends HashSet<E> {
	
	private LinkedList<E> list;
	
	private int maximumSize = 0; //Unilimited

	public SizeLimitedHashSet() {
		this(0);
	}
	
	public SizeLimitedHashSet(int maximunSize) {
		super();
		
		this.maximumSize = maximunSize;
		
		list = new LinkedList<E>();
	}
	
	@Override
	public boolean add(E e) {
		if(super.add(e)){
			if(maximumSize > 0 && list.size() >= maximumSize)
				removeFirst();
			
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
	
	public void removeFirst(){
		super.remove(list.removeFirst());
	}
	
	@Override
	public void clear() {
		super.clear();
		list.clear();
	}
	
	public Collection<? extends E> getInmutableList() {
		return (Collection<? extends E>) Collections.unmodifiableList(list);
	}
}
