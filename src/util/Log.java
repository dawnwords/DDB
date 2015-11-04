package util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Dawnwords on 2015/11/4.
 */
public class Log {

    private static final SimpleDateFormat FORMAT = new SimpleDateFormat("HH:mm:ss SSS");
    private static boolean DEBUG = Boolean.valueOf(System.getProperty("log.debug", "false"));

    public static void i(String format, Object... args) {
        if (DEBUG) {
            System.out.println(format(format, args));
        }
    }

    public static void e(String format, Object... args) {
        System.err.println(format(format, args));
    }

    private static String format(String format, Object... args) {
        Object[] newArgs = new Object[args.length + 2];
        newArgs[0] = FORMAT.format(new Date());
        newArgs[1] = Thread.currentThread().getName();
        System.arraycopy(args, 0, newArgs, 2, args.length);
        return String.format("[%s - %s]" + format, newArgs);
    }
}
