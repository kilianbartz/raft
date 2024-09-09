package org.oxoo2a.sim4da;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class Simulator {
    private final String version = "sim4da V2.0";
    private  Simulator () {
        System.setProperty("PID", String.valueOf(ProcessHandle.current().pid())); // Needed for logback
        logger = LoggerFactory.getLogger(sim4da.class);
        System.out.println(version);
        logger.info(version + " - Simulation started.");
    }

    public static Simulator getInstance() {
        if (instance == null) {
            synchronized (Simulator.class) {
                if (instance == null) {
                    instance = new Simulator();
                }
            }
        }
        return instance;
    }

    public void simulate ( long duration_in_seconds ) {
        simulating = true;
        startSignal.countDown();
        try {
            Thread.sleep(duration_in_seconds * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        simulating = false;
    }

    public void simulate () {
        simulating = true;
        startSignal.countDown();
        List<NetworkConnection> ncs = Network.getInstance().getAllNetworkConnections();
        for (NetworkConnection nc : ncs) {
            nc.join();
        }
    }
    public void shutdown () {
        Network.getInstance().shutdown();
        logger.info(version + " - Simulation ended.");
    }

    public boolean isSimulating() {
        return simulating;
    }
    private static Simulator instance = null;
    private final Logger logger;
    private boolean simulating = false;
    private final CountDownLatch startSignal = new CountDownLatch(1);

    public void awaitSimulationStart() {
        if (simulating) return;
        try {
            startSignal.await();
        }
        catch (InterruptedException e) {}
    }
}