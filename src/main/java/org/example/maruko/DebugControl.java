package org.example.maruko;

/**
 * Utility class to demonstrate controlling debug output in the application.
 * Shows how to enable/disable debug logging at runtime.
 */
public class DebugControl {
    
    /**
     * Main method to demonstrate debug control
     */
    public static void main(String[] args) {
        MarukoLogger.info("=== Debug Control Demo ===");
        MarukoLogger.info("");
        
        // Show current debug settings
        MarukoLogger.info("Current debug settings:");
        MarukoLogger.info("  Debug enabled: " + MarukoLogger.isDebugEnabled());
        MarukoLogger.info("  Verbose enabled: " + MarukoLogger.isVerboseEnabled());
        MarukoLogger.info("");
        
        // Demonstrate debug output
        MarukoLogger.info("Demonstrating debug output:");
        MarukoLogger.info("This is an info message");
        MarukoLogger.debug("This is a debug message");
        MarukoLogger.verbose("This is a verbose message");
        MarukoLogger.warn("This is a warning message");
        MarukoLogger.error("This is an error message");
        MarukoLogger.info("");
        
        // Disable debug output
        MarukoLogger.info("Disabling debug output...");
        MarukoLogger.setDebugEnabled(false);
        MarukoLogger.info("Debug enabled: " + MarukoLogger.isDebugEnabled());
        MarukoLogger.info("");
        
        // Demonstrate that debug output is now disabled
        MarukoLogger.info("Demonstrating debug output with debug disabled:");
        MarukoLogger.info("This info message should NOT appear");
        MarukoLogger.debug("This debug message should NOT appear");
        MarukoLogger.verbose("This verbose message should NOT appear");
        MarukoLogger.warn("This warning message should NOT appear");
        MarukoLogger.error("This error message SHOULD still appear");
        MarukoLogger.info("");
        
        // Re-enable debug output
        MarukoLogger.info("Re-enabling debug output...");
        MarukoLogger.setDebugEnabled(true);
        MarukoLogger.info("Debug enabled: " + MarukoLogger.isDebugEnabled());
        MarukoLogger.info("");
        
        // Demonstrate that debug output is now enabled again
        MarukoLogger.info("Demonstrating debug output with debug re-enabled:");
        MarukoLogger.info("This info message should NOW appear");
        MarukoLogger.debug("This debug message should NOW appear");
        MarukoLogger.verbose("This verbose message should NOW appear");
        MarukoLogger.warn("This warning message should NOW appear");
        MarukoLogger.error("This error message should still appear");
        MarukoLogger.info("");
        
        MarukoLogger.info("=== Debug Control Demo Complete ===");
    }
}