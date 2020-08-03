package com.ppetrov.interview.alm;

/**
 * Instances of IEntityID are used as locking objects for EntityLocker.
 *
 * Implementations should consider to properly override equals and hashCode methods,
 * because EntityLocker's implementation uses instances of this interface as keys in the map.
 */
public interface IEntityID {
}
