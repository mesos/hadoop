package org.apache.hadoop.mapred;

/**
 * @todo What is the difference between variable and fixed resource policy?
 */
public class ResourcePolicyFixed extends ResourcePolicy {
  public ResourcePolicyFixed(MesosScheduler scheduler) {
    super(scheduler);
  }

  /**
   * Computes the number of slots to launch for this offer
   *
   * @return true if the offer is sufficient
   */
  @Override
  public boolean computeSlots() {
    mapSlots = mapSlotsMax;
    reduceSlots = reduceSlotsMax;

    slots = Integer.MAX_VALUE;
    slots = (int) Math.min(slots, (cpus - containerCpus) / slotCpus);
    slots = (int) Math.min(slots, (mem - containerMem) / slotMem);
    slots = (int) Math.min(slots, (disk - containerDisk) / slotDisk);

    // Is this offer too small for even the minimum slots?
    return slots >= 1 && slots >= mapSlots + reduceSlots;
  }
}
