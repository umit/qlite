package com.umitunal;

import com.umitunal.examples.*;

/**
 * Main class that runs all QLite examples.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        System.out.println("=== QLite Examples ===\n");

        // Run all examples
        BasicExample.main(args);
        RetryExample.main(args);
        ScheduledJobsExample.main(args);
        BackgroundWorkersExample.main(args);
        RecoveryExample.main(args);
        KryoExample.main(args);

        System.out.println("\n=== All Examples Complete ===");
    }
}
