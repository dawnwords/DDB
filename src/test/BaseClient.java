package test;

import transaction.Host;
import transaction.WorkflowController;

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
            System.exit(1);
        }
    }

    protected void assertEqual(String test, int actual, int expect) {
        if (expect == actual) {
            System.out.println("[Pass]" + test);
        } else {
            System.err.printf("[Fail]%s: expect %d, actual %d\n", test, expect, actual);
            System.exit(1);
        }
    }

    public void run() {
        try {
            setUp();
            long xid = wc().start();
            run(xid);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void setUp() throws Exception {
    }

    protected abstract void run(long xid) throws Exception;
}
