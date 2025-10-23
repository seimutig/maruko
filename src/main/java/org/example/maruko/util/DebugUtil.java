package org.example.maruko.util;

/**
 * Utility class for controlling debug output in the application.
 * Provides methods to enable/disable debug logging at runtime.
 */
public class DebugUtil {
    // Global flag to control all debug output
    private static boolean DEBUG_ENABLED = Boolean.parseBoolean(
        System.getProperty("maruko.debug.enabled", "true")
    );
    
    // Flag to control verbose/debug level output separately
    private static boolean VERBOSE_ENABLED = Boolean.parseBoolean(
        System.getProperty("maruko.verbose.enabled", "true")
    );
    
    /**
     * Enable or disable all debug output
     */
    public static void setDebugEnabled(boolean enabled) {
        DEBUG_ENABLED = enabled;
    }
    
    /**
     * Enable or disable verbose output
     */
    public static void setVerboseEnabled(boolean enabled) {
        VERBOSE_ENABLED = enabled;
    }
    
    /**
     * Check if debug output is enabled
     */
    public static boolean isDebugEnabled() {
        return DEBUG_ENABLED;
    }
    
    /**
     * Check if verbose output is enabled
     */
    public static boolean isVerboseEnabled() {
        return VERBOSE_ENABLED && DEBUG_ENABLED;
    }
    
    /**
     * Print debug message if debug is enabled
     */
    public static void debug(String message) {
        if (DEBUG_ENABLED) {
            System.out.println("[DEBUG] " + message);
        }
    }
    
    /**
     * Print verbose/debug message if verbose is enabled
     */
    public static void verbose(String message) {
        if (VERBOSE_ENABLED && DEBUG_ENABLED) {
            System.out.println("[VERBOSE] " + message);
        }
    }
    
    /**
     * Print info message (always printed unless completely disabled)
     */
    public static void info(String message) {
        if (DEBUG_ENABLED) {
            System.out.println("[INFO] " + message);
        }
    }
    
    /**
     * Print error message (always printed)
     */
    public static void error(String message) {
        System.err.println("[ERROR] " + message);
    }
    
    /**
     * Print warning message
     */
    public static void warn(String message) {
        if (DEBUG_ENABLED) {
            System.out.println("[WARN] " + message);
        }
    }
    
    /**
     * Print debug message with formatting if debug is enabled
     */
    public static void debug(String format, Object... args) {
        if (DEBUG_ENABLED) {
            System.out.println("[DEBUG] " + String.format(format, args));
        }
    }
    
    /**
     * Print verbose message with formatting if verbose is enabled
     */
    public static void verbose(String format, Object... args) {
        if (VERBOSE_ENABLED && DEBUG_ENABLED) {
            System.out.println("[VERBOSE] " + String.format(format, args));
        }
    }
}