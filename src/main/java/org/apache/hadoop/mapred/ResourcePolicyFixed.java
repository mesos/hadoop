package org.apache.hadoop.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ResourcePolicyFixed extends ResourcePolicy {

  public static final Log LOG = LogFactory.getLog(ResourcePolicyFixed.class);

  public ResourcePolicyFixed(MesosScheduler scheduler) {
    super(scheduler);
  }

  // This method computes the number of slots to launch for this offer, and
  // returns true if the offer is sufficient.
  @Override
  public boolean computeSlots() {
    mapSlots = mapSlotsMax;
    reduceSlots = reduceSlotsMax;

    slots = Integer.MAX_VALUE;
    slots = (int) Math.min(slots, (cpus - containerCpus) / slotCpus);
    slots = (int) Math.min(slots, (mem - containerMem) / slotMem);
    slots = (int) Math.min(slots, (disk - containerDisk) / slotDisk);

    // Is this offer too small for even the minimum slots?
    if (slots < mapSlots + reduceSlots || slots < 1) {
      return false;
    }
    return true;
  }
}
