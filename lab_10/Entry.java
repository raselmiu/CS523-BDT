package lab10;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * NOTE: This code is based on databricks github code, I modified it according to my need
 * This class represents an Apache access log line.
 * See https://databricks.gitbooks.io/databricks-spark-reference-applications/content/logs_analyzer/chapter1/java8/src/main/java/com/databricks/apps/logs/ApacheAccessLog.java
 */
public class Entry implements Serializable {
    private static final Logger logger = Logger.getLogger("Access");
    // Example Apache log line:
    //   127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] "GET /home.html HTTP/1.1" 200 2048
    private static final String LOG_ENTRY_PATTERN =
            // 1:IP  2:client 3:user 4:date time                   5:method 6:req 7:proto   8:respcode 9:size
            "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);
    private final String ipAddress;
    private final String client;
    private final String userID;
    private final String dateTimeString;
    private final String method;
    private final String endpoint;
    private final String protocol;
    private final int responseCode;
    private final long contentSize;
    private final LocalDate logDate;

    private Entry(String ipAddress, String client, String userID,
                  String dateTime, String method, String endpoint,
                  String protocol, String responseCode,
                  String contentSize) {
        this.ipAddress = ipAddress;
        this.client = client;
        this.userID = userID;
        this.dateTimeString = dateTime;
        this.method = method;
        this.endpoint = endpoint;
        this.protocol = protocol;
        this.responseCode = Integer.parseInt(responseCode);
        this.contentSize = Long.parseLong(contentSize);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss");
        this.logDate = LocalDate.parse(this.dateTimeString.split(" ")[0], formatter);
    }

    public static Optional<Entry> parseFromLogLine(String logLine) {
        Matcher m = PATTERN.matcher(logLine);
        if (!m.find()) {
            logger.log(Level.ALL, "Cannot parse logLine" + logLine);
            return Optional.empty();
        }

        Entry logEntry = new Entry(m.group(1), m.group(2),
                m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8), m.group(9));

        return Optional.of(logEntry);
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public String getClient() {
        return client;
    }

    public String getUserID() {
        return userID;
    }

    public String getDateTimeString() {
        return dateTimeString;
    }

    public String getMethod() {
        return method;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getProtocol() {
        return protocol;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public long getContentSize() {
        return contentSize;
    }

    public LocalDate getLogDate() {
        return logDate;
    }
}