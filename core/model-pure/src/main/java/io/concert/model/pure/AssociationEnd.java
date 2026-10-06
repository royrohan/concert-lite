package io.concert.model.pure;

/**
 * An association seen from one participating class.
 *
 * @param navigable the property this class gets from the association
 * @param opposite the property the class at the other end gets (pointing back to this class)
 * @param owned whether {@code navigable} carries the related objects when serialized; see
 *     {@link ResolvedModel} for the ownership rule
 */
public record AssociationEnd(AssociationDef association, PropertyDef navigable, PropertyDef opposite, boolean owned) {}
