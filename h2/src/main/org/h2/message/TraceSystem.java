/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.message;

import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.jdbc.JdbcException;
import org.h2.store.fs.FileUtils;
import org.h2.util.IOUtils;

/**
 * The trace mechanism is the logging facility of this database. There is
 * usually one trace system per database. It is called 'trace' because the term
 * 'log' is already used in the database domain and means 'transaction log'. It
 * is possible to write after close was called, but that means for each write
 * the file will be opened and closed again (which is slower).
 */
public class TraceSystem implements TraceWriter {

    /**
     * The parent trace level should be used.
     */
    public static final int PARENT = -1;

    /**
     * This trace level means nothing should be written.
     */
    public static final int OFF = 0;

    /**
     * This trace level means only errors should be written.
     */
    public static final int ERROR = 1;

    /**
     * This trace level means errors and informational messages should be
     * written.
     */
    public static final int INFO = 2;

    /**
     * This trace level means all type of messages should be written.
     */
    public static final int DEBUG = 3;

    /**
     * This trace level means all type of messages should be written, but
     * instead of using the trace file the messages should be written to SLF4J.
     */
    public static final int ADAPTER = 4;

    /**
     * The default level for system out trace messages.
     */
    public static final int DEFAULT_TRACE_LEVEL_SYSTEM_OUT = OFF;

    /**
     * The default level for file trace messages.
     */
    public static final int DEFAULT_TRACE_LEVEL_FILE = ERROR;

    /**
     * The default maximum trace file size. It is currently 64 MB. Additionally,
     * there could be a .old file of the same size.
     */
    private static final int DEFAULT_MAX_FILE_SIZE = 64 * 1024 * 1024;

    private static final int CHECK_SIZE_EACH_WRITES = 4096;

    private static DateTimeFormatter DATE_TIME_FORMATTER;

    private int levelSystemOut = DEFAULT_TRACE_LEVEL_SYSTEM_OUT;
    private int levelFile = DEFAULT_TRACE_LEVEL_FILE;
    private int levelMax;
    private int maxFileSize = DEFAULT_MAX_FILE_SIZE;
    private String fileName;
    private final AtomicReferenceArray<Trace> traces =
            new AtomicReferenceArray<>(Trace.MODULE_NAMES.length);
    private Writer fileWriter;
    private PrintWriter printWriter;
    /**
     * Starts at -1 so that we check the file size immediately upon open. This
     * Can be important if we open and close the trace file without managing to
     * have written CHECK_SIZE_EACH_WRITES bytes each time.
     */
    private int checkSize = -1;
    private boolean closed;
    private boolean writingErrorLogged;
    private TraceWriter writer = this;
    private PrintStream sysOut = System.out;

    /**
     * Create a new trace system object.
     *
     * @param fileName the file name
     */
    public TraceSystem(String fileName) {
        this.fileName = fileName;
        updateLevel();
    }

    private void updateLevel() {
        levelMax = Math.max(levelSystemOut, levelFile);
    }

    /**
     * Set the print stream to use instead of System.out.
     *
     * @param out the new print stream
     */
    public void setSysOut(PrintStream out) {
        this.sysOut = out;
    }

    /**
     * Get or create a trace object for this module id. Trace modules with id
     * are cached.
     *
     * @param moduleId module id
     * @return the trace object
     */
    public Trace getTrace(int moduleId) {
        Trace t = traces.get(moduleId);
        if (t == null) {
            t = new Trace(writer, moduleId);
            if (!traces.compareAndSet(moduleId, null, t)) {
                t = traces.get(moduleId);
            }
        }
        return t;
    }

    /**
     * Create a trace object for this module. Trace modules with names are not
     * cached.
     *
     * @param module the module name
     * @return the trace object
     */
    public Trace getTrace(String module) {
        return new Trace(writer, module);
    }

    @Override
    public boolean isEnabled(int level) {
        if (levelMax == ADAPTER) {
            return writer.isEnabled(level);
        }
        return level <= this.levelMax;
    }

    /**
     * Set the trace file name.
     *
     * @param name the file name
     */
    public void setFileName(String name) {
        this.fileName = name;
    }

    /**
     * Set the maximum trace file size in bytes.
     *
     * @param max the maximum size
     */
    public void setMaxFileSize(int max) {
        this.maxFileSize = max;
    }

    /**
     * Set the trace level to use for System.out
     *
     * @param level the new level
     */
    public void setLevelSystemOut(int level) {
        if (level < PARENT || level > DEBUG) {
            throw DbException.getInvalidValueException("TRACE_LEVEL_SYSTEM_OUT", level);
        }
        levelSystemOut = level;
        updateLevel();
    }

    /**
     * Set the file trace level.
     *
     * @param level the new level
     */
    public void setLevelFile(int level) {
        if (level == ADAPTER) {
            String adapterClass = "org.h2.message.TraceWriterAdapter";
            try {
                writer = (TraceWriter) Class.forName(adapterClass).getDeclaredConstructor().newInstance();
            } catch (Throwable e) {
                e = DbException.get(ErrorCode.CLASS_NOT_FOUND_1, e, adapterClass);
                write(ERROR, Trace.DATABASE, adapterClass, e);
                return;
            }
            String name = fileName;
            if (name != null) {
                if (name.endsWith(Constants.SUFFIX_TRACE_FILE)) {
                    name = name.substring(0, name.length() - Constants.SUFFIX_TRACE_FILE.length());
                }
                int idx = Math.max(name.lastIndexOf('/'), name.lastIndexOf('\\'));
                if (idx >= 0) {
                    name = name.substring(idx + 1);
                }
                writer.setName(name);
            }
        } else if (level < PARENT || level > DEBUG) {
            throw DbException.getInvalidValueException("TRACE_LEVEL_FILE", level);
        }
        levelFile = level;
        updateLevel();
    }

    public int getLevelFile() {
        return levelFile;
    }

    private static String format(String module, String s) {
        DateTimeFormatter dateTimeFormatter = DATE_TIME_FORMATTER;
        if (dateTimeFormatter == null) {
            dateTimeFormatter = initTimeFormatter();
        }
        return dateTimeFormatter.format(OffsetDateTime.now()) + ' ' + module + ": " + s;
    }

    private static DateTimeFormatter initTimeFormatter() {
        return DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
                .append(DateTimeFormatter.ISO_LOCAL_DATE)
                .appendLiteral(' ')
                .appendValue(HOUR_OF_DAY, 2)
                .appendLiteral(':')
                .appendValue(MINUTE_OF_HOUR, 2)
                .appendLiteral(':')
                .appendValue(SECOND_OF_MINUTE, 2)
                .appendFraction(NANO_OF_SECOND, 6, 6, true)
                .appendOffsetId()
                .toFormatter(Locale.ROOT);
    }

    @Override
    public void write(int level, int moduleId, String s, Throwable t) {
        write(level, Trace.MODULE_NAMES[moduleId], s, t);
    }

    @Override
    public void write(int level, String module, String s, Throwable t) {
        // level <= levelSystemOut: the system out level is set higher
        // level > levelMax: the level for this module is set higher
        boolean logToSystemOut = level <= levelSystemOut || level > levelMax;
        boolean logToFile = fileName != null && level <= levelFile;
        if (logToSystemOut || logToFile) {
            String row = format(module, s);
            if (logToSystemOut) {
                sysOut.println(row);
                if (t != null && levelSystemOut == DEBUG) {
                    t.printStackTrace(sysOut);
                }
            }
            if (logToFile) {
                writeFile(row, t);
            }
        }
    }

    private synchronized void writeFile(String s, Throwable t) {
        try {
            checkSize = (checkSize + 1) % CHECK_SIZE_EACH_WRITES;
            if (checkSize == 0) {
                closeWriter();
                if (maxFileSize > 0 && FileUtils.size(fileName) > maxFileSize) {
                    String old = fileName + ".old";
                    FileUtils.delete(old);
                    FileUtils.move(fileName, old);
                }
            }
            if (!openWriter()) {
                return;
            }
            printWriter.println(s);
            if (t != null) {
                if (levelFile == ERROR && t instanceof JdbcException) {
                    JdbcException se = (JdbcException) t;
                    int code = se.getErrorCode();
                    if (ErrorCode.isCommon(code)) {
                        printWriter.println(t);
                    } else {
                        t.printStackTrace(printWriter);
                    }
                } else {
                    t.printStackTrace(printWriter);
                }
            }
            printWriter.flush();
            if (closed) {
                closeWriter();
            }
        } catch (Exception e) {
            logWritingError(e);
        }
    }

    private void logWritingError(Exception e) {
        if (writingErrorLogged) {
            return;
        }
        writingErrorLogged = true;
        Exception se = DbException.get(
                ErrorCode.TRACE_FILE_ERROR_2, e, fileName, e.toString());
        // print this error only once
        fileName = null;
        sysOut.println(se);
        se.printStackTrace();
    }

    private boolean openWriter() {
        if (printWriter == null) {
            try {
                FileUtils.createDirectories(FileUtils.getParent(fileName));
                if (FileUtils.exists(fileName) && !FileUtils.canWrite(fileName)) {
                    // read only database: don't log error if the trace file
                    // can't be opened
                    return false;
                }
                fileWriter = IOUtils.getBufferedWriter(
                        FileUtils.newOutputStream(fileName, true));
                printWriter = new PrintWriter(fileWriter, true);
            } catch (Exception e) {
                logWritingError(e);
                return false;
            }
        }
        return true;
    }

    private synchronized void closeWriter() {
        if (printWriter != null) {
            printWriter.flush();
            printWriter.close();
            printWriter = null;
        }
        if (fileWriter != null) {
            try {
                fileWriter.close();
            } catch (IOException e) {
                // ignore
            }
            fileWriter = null;
        }
    }

    /**
     * Close the writers, and the files if required. It is still possible to
     * write after closing, however after each write the file is closed again
     * (slowing down tracing).
     */
    public void close() {
        closeWriter();
        closed = true;
    }

    @Override
    public void setName(String name) {
        // nothing to do (the file name is already set)
    }

}
