package lockmgr;

/**
 * Created by Dawnwords on 2015/11/2.
 */
public enum LockType {
    WRITE, READ;

    public boolean isShared(LockType type) {
        return this == READ && type == READ;
    }
}
