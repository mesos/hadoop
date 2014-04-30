package org.apache.hadoop.mapred;

public class ResourcePolicyVariable extends ResourcePolicy {
    public ResourcePolicyVariable(MesosScheduler scheduler) {
      super(scheduler);
    }

    // This method computes the number of slots to launch for this offer, and
    // returns true if the offer is sufficient.
    @Override
    public boolean computeSlots() {
      // What's the minimum number of map and reduce slots we should try to
      // launch?
      mapSlots = 0;
      reduceSlots = 0;

      // Determine how many slots we can allocate.
      int slots = mapSlotsMax + reduceSlotsMax;
      slots = (int) Math.min(slots, (cpus - containerCpus) / slotCpus);
      slots = (int) Math.min(slots, (mem - containerMem) / slotMem);
      slots = (int) Math.min(slots, (disk - containerDisk) / slotDisk);

      // Is this offer too small for even the minimum slots?
      if (slots < 1) {
        return false;
      }

      // Is the number of slots we need sufficiently small? If so, we can
      // allocate exactly the number we need.
      if (slots >= neededMapSlots + neededReduceSlots && neededMapSlots <
          mapSlotsMax && neededReduceSlots < reduceSlotsMax) {
        mapSlots = neededMapSlots;
        reduceSlots = neededReduceSlots;
      } else {
        // Allocate slots fairly for this resource offer.
        double mapFactor = (double) neededMapSlots / (neededMapSlots + neededReduceSlots);
        double reduceFactor = (double) neededReduceSlots / (neededMapSlots + neededReduceSlots);
        // To avoid map/reduce slot starvation, don't allow more than 50%
        // spread between map/reduce slots when we need both mappers and
        // reducers.
        if (neededMapSlots > 0 && neededReduceSlots > 0) {
          if (mapFactor < 0.25) {
            mapFactor = 0.25;
          } else if (mapFactor > 0.75) {
            mapFactor = 0.75;
          }
          if (reduceFactor < 0.25) {
            reduceFactor = 0.25;
          } else if (reduceFactor > 0.75) {
            reduceFactor = 0.75;
          }
        }
        mapSlots = Math.min(Math.min((long)Math.max(Math.round(mapFactor * slots), 1), mapSlotsMax), neededMapSlots);

        // The remaining slots are allocated for reduces.
        slots -= mapSlots;
        reduceSlots = Math.min(Math.min(slots, reduceSlotsMax), neededReduceSlots);
      }
      return true;
    }
  }
