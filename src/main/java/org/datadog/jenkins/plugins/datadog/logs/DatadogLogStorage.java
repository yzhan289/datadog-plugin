/*
The MIT License

Copyright (c) 2015-Present Datadog, Inc <opensource@datadoghq.com>
All rights reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
 */


package org.datadog.jenkins.plugins.datadog.logs;

import hudson.console.AnnotatedLargeText;
import hudson.console.ConsoleAnnotationOutputStream;
import hudson.model.BuildListener;
import hudson.model.Run;
import hudson.model.TaskListener;
import hudson.util.ByteArrayOutputStream2;
import org.apache.commons.io.input.NullReader;
import org.jenkinsci.plugins.workflow.flow.FlowExecutionOwner;
import org.jenkinsci.plugins.workflow.graph.FlowNode;
import org.jenkinsci.plugins.workflow.log.BrokenLogStorage;
import org.jenkinsci.plugins.workflow.log.ConsoleAnnotators;
import org.jenkinsci.plugins.workflow.log.LogStorage;
import org.kohsuke.stapler.framework.io.ByteBuffer;

import javax.annotation.Nonnull;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DatadogLogStorage implements LogStorage {

    private static final Logger LOGGER = Logger.getLogger(DatadogLogStorage.class.getName());
    private FileOutputStream os;
    private final File log;
    private final File index;
    private static final Map<File, DatadogLogStorage> openStorages = Collections
            .synchronizedMap(new HashMap<>());
    private OutputStream bos;
    private String lastId;
    private Writer indexOs;
    private static final int LF = 0x0A;
    private Run<?, ?> run;

    public DatadogLogStorage(File log, Run<?, ?> run) {
        this.log = log;
        this.index = new File(log + "-index");
        this.run = run;
    }

    public static synchronized LogStorage forFile(File log, Run<?, ?> run) {

        return openStorages.computeIfAbsent(log, t -> new DatadogLogStorage(log, run));
    }

    @Override
    @Nonnull
    public BuildListener overallListener() throws IOException, InterruptedException {
        return new DatadogBuildListener(new DatadogLogStorage.IndexOutputStream(null));
    }

    @Override
    @Nonnull
    public TaskListener nodeListener(FlowNode flowNode) throws IOException, InterruptedException {
        return new DatadogBuildListener(new DatadogLogStorage.IndexOutputStream(flowNode.getId()));
    }

    @Override
    @Nonnull
    public AnnotatedLargeText<FlowExecutionOwner.Executable> overallLog(FlowExecutionOwner.Executable build, boolean complete) {
        maybeFlush();
        return new AnnotatedLargeText<FlowExecutionOwner.Executable>(log, StandardCharsets.UTF_8, complete, build) {

            @Override
            public long writeHtmlTo(long start, Writer w) throws IOException {
                try (BufferedReader indexBR = index.isFile() ? Files.newBufferedReader(index.toPath(), StandardCharsets.UTF_8) : new BufferedReader(new NullReader(0))) {
                    ConsoleAnnotationOutputStream<FlowExecutionOwner.Executable> caos = new ConsoleAnnotationOutputStream<>(w, ConsoleAnnotators
                            .createAnnotator(build), build, StandardCharsets.UTF_8);
                    long r = this.writeRawLogTo(start, new FilterOutputStream(caos) {
                        long lastTransition = -1;
                        boolean eof; // NullReader is strict and throws IOException (not EOFException) if you read() again after having already gotten -1
                        String lastId;
                        long pos = start;
                        boolean hadLastId;

                        @Override
                        public void write(int b) throws IOException {
                            while (lastTransition < pos && !eof) {
                                String line = indexBR.readLine();
                                if (line == null) {
                                    eof = true;
                                    break;
                                }
                                int space = line.indexOf(' ');
                                try {
                                    lastTransition = Long.parseLong(space == -1 ? line : line.substring(0, space));
                                } catch (NumberFormatException x) {
                                    LOGGER.warning("Ignoring corrupt index file " + index);
                                }
                                lastId = space == -1 ? null : line.substring(space + 1);
                            }
                            if (pos == lastTransition) {
                                if (hadLastId) {
                                    w.write(LogStorage.endStep());
                                }
                                hadLastId = lastId != null;
                                if (lastId != null) {
                                    w.write(LogStorage.startStep(lastId));
                                }
                            }
                            super.write(b);
                            pos++;
                        }

                        @Override
                        public void flush() throws IOException {
                            if (lastId != null) {
                                w.write(LogStorage.endStep());
                            }
                            super.flush();
                        }
                    });
                    ConsoleAnnotators.setAnnotator(caos.getConsoleAnnotator());
                    return r;
                }
            }
        };
    }

    @Override
    @Nonnull
    public AnnotatedLargeText<FlowNode> stepLog(FlowNode node, boolean complete) {
        maybeFlush();
        String id = node.getId();
        try (ByteBuffer buf = new ByteBuffer();
             RandomAccessFile raf = new RandomAccessFile(log, "r");
             BufferedReader indexBR = index.isFile() ? Files.newBufferedReader(index.toPath(), StandardCharsets.UTF_8) : new BufferedReader(new NullReader(0))) {

            long end = raf.length();
            String line;
            long pos = -1; // -1 if not currently in this node, start position if we are
            while ((line = indexBR.readLine()) != null) {
                int space = line.indexOf(' ');
                long lastTransition = -1;
                try {
                    lastTransition = Long.parseLong(space == -1 ? line : line.substring(0, space));
                } catch (NumberFormatException x) {
                    LOGGER.warning("Ignoring corrupt index file " + index);
                    continue;
                }
                if (pos == -1) {
                    if (space != -1 && line.substring(space + 1).equals(id)) {
                        pos = lastTransition;
                    }
                } else if (lastTransition > pos) {
                    raf.seek(pos);
                    if (lastTransition > pos + Integer.MAX_VALUE) {
                        throw new IOException("Cannot read more than 2Gib at a time"); // ByteBuffer does not support it anyway
                    }
                    byte[] data = new byte[(int) (lastTransition - pos)];
                    raf.readFully(data);
                    buf.write(data);
                    pos = -1;
                }
            }
            if (pos != -1 && /* otherwise race condition? */ end > pos) {
                // In case the build is ongoing and we are still actively writing content for this step,
                // we will hit EOF before any other transition. Otherwise identical to normal case above.
                raf.seek(pos);
                if (end > pos + Integer.MAX_VALUE) {
                    throw new IOException("Cannot read more than 2Gib at a time");
                }
                byte[] data = new byte[(int) (end - pos)];
                raf.readFully(data);
                buf.write(data);
            }
            return new AnnotatedLargeText<>(buf, StandardCharsets.UTF_8, complete, node);
        } catch (IOException x) {
            return new BrokenLogStorage(x).stepLog(node, complete);
        }
    }

    private void maybeFlush() {
        if (bos != null) {
            try {
                bos.flush();
            } catch (IOException x) {
                LOGGER.log(Level.WARNING, "failed to flush " + log, x);
            }
        }
    }

    private synchronized void open() throws IOException {
        if (os == null) {
            os = new FileOutputStream(log, true);

            DatadogWriter writer = new DatadogWriter(run, null, run.getCharset());
            bos = new DatadogOutputStream(new DelayBufferedOutputStream(os), writer);

            if (index.isFile()) {
                try (BufferedReader r = Files.newBufferedReader(index.toPath(), StandardCharsets.UTF_8)) {
                    String lastLine = null;
                    while (true) {
                        String line = r.readLine();
                        if (line == null) {
                            break;
                        } else {
                            lastLine = line;
                        }
                    }
                    if (lastLine != null) {
                        int space = lastLine.indexOf(' ');
                        lastId = space == -1 ? null : lastLine.substring(space + 1);
                    }
                }
            }
            indexOs = new OutputStreamWriter(new FileOutputStream(index, true), StandardCharsets.UTF_8);
        }
    }

    private void checkId(String id) throws IOException {
        assert Thread.holdsLock(this);
        if (!Objects.equals(id, lastId)) {
            bos.flush();
            long pos = os.getChannel().position();
            if (id == null) {
                indexOs.write(pos + "\n");
            } else {
                indexOs.write(pos + " " + id + "\n");
            }
            // Could call FileChannel.force(true) like hudson.util.FileChannelWriter does for AtomicFileWriter,
            // though making index-log writes slower is likely a poor tradeoff for slightly more reliable log display,
            // since logs are often never read and this is transient data rather than configuration or valuable state.
            indexOs.flush();
            lastId = id;
        }
    }

    private final class IndexOutputStream extends OutputStream {
        private final String id;

        private ByteArrayOutputStream2 buf = new ByteArrayOutputStream2();

        IndexOutputStream(String id) throws IOException {
            this.id = id;
            open();
        }

        @Override public void write(int b) throws IOException {
            synchronized (DatadogLogStorage.this) {
                checkId(id);
                writeColorLog(b);
            }
        }

        @Override public void write(byte[] b) throws IOException {
            synchronized (DatadogLogStorage.this) {
                checkId(id);
                writeColorLog(b);
            }
        }

        @Override public void write(byte[] b, int off, int len) throws IOException {
            synchronized (DatadogLogStorage.this) {
                checkId(id);
                writeColorLog(b, off, len);
            }
        }

        @Override public void flush() throws IOException {
            bos.flush();
        }

        @Override public void close() throws IOException {
            if (id == null) {
                openStorages.remove(log);
                try {
                    bos.close();
                } finally {
                    indexOs.close();
                }
            }
        }

        private void writeColorLog(int b) throws IOException {
            buf.write(b);
            if (b==LF) eol();
        }

        private void writeColorLog(byte[] b) throws IOException {
            for (byte value : b) writeColorLog(value);
        }

        private void writeColorLog(byte[] b, int off, int len) throws IOException {
            int end = off+len;

            for( int i=off; i<end; i++ )
                writeColorLog(b[i]);
        }

        private void eol() throws IOException {
            eol(buf.getBuffer(), buf.size());

            // reuse the buffer under normal circumstances, but don't let the line buffer grow unbounded
            if (buf.size()>4096)
                buf = new ByteArrayOutputStream2();
            else
                buf.reset();
        }

        private void eol(byte[] bytes, int len) throws IOException {
            final String inputLine = new String(bytes, 0, len, StandardCharsets.UTF_8);
            String line = inputLine;

            bos.write(line.getBytes(StandardCharsets.UTF_8));

        }
    }
}
