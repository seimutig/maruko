package org.example.maruko;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Universal logger for Maruko that supports both standalone and Flink cluster environments.
 * 
 * In standalone mode: Uses simplelogger.properties for configuration
 * In Flink mode: Integrates with Flink's Log4j configuration system
 * 
 * Usage:
 * - Standalone: Works out-of-box with simple configuration
 * - Flink Cluster: Automatically inherits Flink's logging settings
 */
public class MarukoLogger {
    
    // Delegate logger that works in both environments
    private static final Logger DELEGATE_LOGGER = LoggerFactory.getLogger("MarukoTableStore");
    
    /**
     * Get a logger for a specific class that works in both standalone and Flink environments
     */
    public static Logger getLogger(Class<?> clazz) {
        return LoggerFactory.getLogger(clazz);
    }
    
    /**
     * Get a named logger that works in both environments
     */
    public static Logger getLogger(String name) {
        return LoggerFactory.getLogger(name);
    }
    
    // === 兼容原有DebugLogger API的方法 ===
    
    /**
     * Print debug message - compatible with existing DebugLogger.debug() calls
     */
    public static void debug(String message) {
        DELEGATE_LOGGER.debug(message);
    }
    
    /**
     * Print verbose/debug message - compatible with existing DebugLogger.verbose() calls
     */
    public static void verbose(String message) {
        DELEGATE_LOGGER.debug(message); // Verbose is treated as debug in SLF4J
    }
    
    /**
     * Print info message - compatible with existing DebugLogger.info() calls
     */
    public static void info(String message) {
        DELEGATE_LOGGER.info(message);
    }
    
    /**
     * Print warning message - compatible with existing DebugLogger.warn() calls
     */
    public static void warn(String message) {
        DELEGATE_LOGGER.warn(message);
    }
    
    /**
     * Print error message - compatible with existing DebugLogger.error() calls
     */
    public static void error(String message) {
        DELEGATE_LOGGER.error(message);
    }
    
    /**
     * Print debug message with formatting - compatible with existing DebugLogger.debug() calls
     */
    public static void debug(String format, Object... args) {
        DELEGATE_LOGGER.debug(format, args);
    }
    
    /**
     * Print verbose message with formatting - compatible with existing DebugLogger.verbose() calls
     */
    public static void verbose(String format, Object... args) {
        DELEGATE_LOGGER.debug(format, args); // Verbose is treated as debug in SLF4J
    }
    
    /**
     * Print info message with formatting - compatible with existing DebugLogger.info() calls
     */
    public static void info(String format, Object... args) {
        DELEGATE_LOGGER.info(format, args);
    }
    
    /**
     * Print warning message with formatting - compatible with existing DebugLogger.warn() calls
     */
    public static void warn(String format, Object... args) {
        DELEGATE_LOGGER.warn(format, args);
    }
    
    /**
     * Print error message with formatting - compatible with existing DebugLogger.error() calls
     */
    public static void error(String format, Object... args) {
        DELEGATE_LOGGER.error(format, args);
    }
    
    // === 控制方法（在Flink环境中为no-op）===
    
    /**
     * Enable or disable debug output (no-op in Flink, controlled by Flink config)
     * In standalone mode: Controls whether debug messages are printed
     * In Flink mode: No effect, controlled by Flink's logging configuration
     */
    public static void setDebugEnabled(boolean enabled) {
        // No-op: In Flink environment, this is controlled by Flink's logging configuration
        // In standalone mode, users should use simplelogger.properties or system properties
    }
    
    /**
     * Enable or disable verbose output (no-op in Flink, controlled by Flink config)
     * In standalone mode: Controls whether verbose messages are printed
     * In Flink mode: No effect, controlled by Flink's logging configuration
     */
    public static void setVerboseEnabled(boolean enabled) {
        // No-op: In Flink environment, this is controlled by Flink's logging configuration
        // In standalone mode, verbose is treated as debug and controlled by debug level
    }
    
    /**
     * Check if debug output is enabled
     * In standalone mode: Returns true if debug level is enabled
     * In Flink mode: Returns true if Flink's debug level is enabled
     */
    public static boolean isDebugEnabled() {
        return DELEGATE_LOGGER.isDebugEnabled();
    }
    
    /**
     * Check if verbose output is enabled
     * In standalone mode: Returns true if debug level is enabled (verbose treated as debug)
     * In Flink mode: Returns true if Flink's debug level is enabled
     */
    public static boolean isVerboseEnabled() {
        return DELEGATE_LOGGER.isDebugEnabled(); // Verbose is treated as debug in SLF4J
    }
}