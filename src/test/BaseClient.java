package test;

import transaction.Host;
import transaction.WorkflowController;

import java.io.File;
import java.io.FileInputStream;
import java.rmi.Naming;
import java.util.Properties;

/**
 * Created by Dawnwords on 2015/12/24.
 */
public abstract class BaseClient {
    private WorkflowController wc;

    protected WorkflowController wc() {
        if (wc == null) {
            Properties prop = new Properties();
            try {
                prop.load(new FileInputStream("conf/ddb.conf"));
            } catch (Exception e) {
                e.printStackTrace();
            }
            String rmiPort = prop.getProperty("wc.port");
            if (rmiPort == null) {
                rmiPort = "";
            } else if (!rmiPort.equals("")) {
                rmiPort = "//:" + rmiPort + "/";
            }

            try {
                wc = (WorkflowController) Naming.lookup(rmiPort + Host.HostName.WC);
                System.out.println("Bound to WC");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return wc;
    }

    protected void assertTrue(String test, boolean b) {
        if (b) {
            System.out.println("[Pass]" + test);
        } else {
            System.err.println("[Fail]" + test);
        }
    }

    protected void assertEqual(String test, int actual, int expect) {
        if (expect == actual) {
            System.out.println("[Pass]" + test);
        } else {
            System.err.printf("[Fail]%s: expect %d, actual %d\n", test, expect, actual);
        }
    }

    public void run() {
        try {
            setUp();
            long xid = wc().start();
            run(xid);
            assertTrue("Commit", wc().commit(xid));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void setUp() {
        delete(new File("tm.log"));
        delete(new File("RMCars"));
        delete(new File("RMCustomers"));
        delete(new File("RMFlights"));
        delete(new File("RMReservations"));
        delete(new File("RMRooms"));
    }

    private void delete(File file) {
        if (file.exists()) {
            if (file.isFile()) {
                file.delete();
            } else if (file.isDirectory()) {
                File[] files = file.listFiles();
                if (files != null) {
                    for (File file1 : files) {
                        delete(file1);
                    }
                }
                file.delete();
            }
        } else {
            System.out.println("所删除的文件不存在");
        }
    }

    protected abstract void run(long xid) throws Exception;
}
