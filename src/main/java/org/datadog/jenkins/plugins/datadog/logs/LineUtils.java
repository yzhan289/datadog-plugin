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

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class LineUtils {
    private static Logger LOGGER = Logger.getLogger(LineUtils.class.getCanonicalName());

    private static final Pattern WARNING = Pattern.compile("WARN", Pattern.CASE_INSENSITIVE);
    private static final Pattern ERROR = Pattern.compile("ERROR", Pattern.CASE_INSENSITIVE);

    private static final String WARNING_SPAN_PREFIX = "<span style=\"color: #fff; background-color: #f57542; font-size: 100%; font-weight: bold\">";
    private static final String ERROR_SPAN_PREFIX = "<span style=\"color: #fff; background-color: #ed0e0e; font-size: 100%; font-weight: bold\">";
    private static final String SPAN_SUFFIX = "</span>";

    public static String filterLine(String line) {
        String result = line;
//        try {
//            StringBuilder sb = new StringBuilder(line);
//            if (WARNING.matcher(line).find()) {
//                sb.insert(0, WARNING_SPAN_PREFIX);
//                sb.insert(sb.lastIndexOf("\n"), SPAN_SUFFIX);
//            } else if (ERROR.matcher(line).find()) {
//                sb.insert(0, ERROR_SPAN_PREFIX);
//                sb.insert(sb.lastIndexOf("\n"), SPAN_SUFFIX);
//            }
//            result = sb.toString();
//        } catch (Exception e) {
//            LOGGER.log(Level.WARNING,
//                    "Exception when wrapping log output" +
//                            " in line '" + line + "' got: '" + e + "'.", e);
//        }
        return result;
    }

    public static void main(String[] args) {
    }
}
