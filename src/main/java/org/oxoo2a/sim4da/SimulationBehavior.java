package org.oxoo2a.sim4da;

import java.util.function.Supplier;

public class SimulationBehavior {

    // Distribution function for the selection of the next message in a message queue
    private static RandomValues r_message_queue_selection = null;
    public static void setMessageQueueSelectionDistributionFunction ( Supplier<Double> df_message_queue_selection ) {
        if (r_message_queue_selection == null) {
            r_message_queue_selection = new RandomValues(df_message_queue_selection);
        }
        else {
            System.err.println("Distribution function for message queue selection has already been set.");
            System.exit(-1);
        }
    }
    public static int selectMessageInQueue ( int queue_size ) {
        assert(queue_size > 0);
        if (r_message_queue_selection == null) {
            return 0;
        }
        else {
            return (int) r_message_queue_selection.getLong(0, queue_size-1);
        }
    }
}
