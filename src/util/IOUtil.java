package util;

import java.io.*;

/**
 * Created by Dawnwords on 2015/12/16.
 */
public class IOUtil {
    public static boolean writeObject(String parentDir, String fileName, Object data) {
        File parent = new File(parentDir);
        if (!parent.exists() || !parent.isDirectory()) {
            parent.mkdirs();
        }
        return writeObject(parentDir + File.separator + fileName, data);
    }

    public static <T> T readObject(String fileName) {
        ObjectInputStream in = null;
        try {
            in = new ObjectInputStream(new FileInputStream(fileName));
            return (T) in.readObject();
        } catch (Exception e) {
            return null;
        } finally {
            close(in);
        }
    }

    public static boolean writeObject(String fileName, Object data) {
        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(new FileOutputStream(fileName));
            out.writeObject(data);
            out.flush();
            return true;
        } catch (Exception e) {
            return false;
        } finally {
            close(out);
        }
    }

    private static void close(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException ignored) {
        }
    }
}
