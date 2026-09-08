/**
 * Whether a layer belonging to a bundle may be edited.
 *
 * Editability is a property of the bundle type's role, resolved server-side and
 * carried on the member listing — the client never encodes the rules. A member
 * whose role is not editable keeps every other layer action; it only loses
 * feature editing, because editing it would drive the bundle's derived
 * artifacts in ways nobody has designed yet.
 */

export type BundleMemberGate = { role: string | null; editable?: boolean };

/** Whether a layer is a bundle member whose role may be edited. */
export function isEditableBundleMember(
  layerId: string,
  membersByLayerId: Map<string, BundleMemberGate> | undefined
): boolean {
  return !!membersByLayerId?.get(layerId)?.editable;
}

/** Whether a layer is a bundle member at all, editable or not. */
export function isBundleMemberLayer(
  layerId: string,
  membersByLayerId: Map<string, BundleMemberGate> | undefined
): boolean {
  return !!membersByLayerId?.has(layerId);
}
