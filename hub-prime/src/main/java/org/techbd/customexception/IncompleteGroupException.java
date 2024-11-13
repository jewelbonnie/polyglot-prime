package org.techbd.customexception;

/**
 * An exception that is thrown when a group of files is incomplete, i.e., not
 * all
 * the required file types are present in the group.
 * 
 * This exception is used in the {@link ZipProcessingTasklet} class to handle
 * cases where the required files (DEMOGRAPHIC_DATA, QE_ADMIN_DATA, SCREENING)
 * are not all present in a group of files.
 */
public class IncompleteGroupException extends RuntimeException {
    private final String groupId;
    private final boolean hasDemographic;
    private final boolean hasQeAdmin;
    private final boolean hasScreening;

    public IncompleteGroupException(String groupId, boolean hasDemographic, boolean hasQeAdmin, boolean hasScreening) {
        super(String.format("Incomplete file group found for groupId: %s. Missing files: %s",
                groupId,
                getMissingFiles(hasDemographic, hasQeAdmin, hasScreening)));
        this.groupId = groupId;
        this.hasDemographic = hasDemographic;
        this.hasQeAdmin = hasQeAdmin;
        this.hasScreening = hasScreening;
    }

    private static String getMissingFiles(boolean hasDemographic, boolean hasQeAdmin, boolean hasScreening) {
        StringBuilder missing = new StringBuilder();
        if (!hasDemographic)
            missing.append("DEMOGRAPHIC_DATA, ");
        if (!hasQeAdmin)
            missing.append("QE_ADMIN_DATA, ");
        if (!hasScreening)
            missing.append("SCREENING, ");
        return missing.length() > 0 ? missing.substring(0, missing.length() - 2) : "";
    }

    public String getGroupId() {
        return groupId;
    }

    public boolean hasDemographic() {
        return hasDemographic;
    }

    public boolean hasQeAdmin() {
        return hasQeAdmin;
    }

    public boolean hasScreening() {
        return hasScreening;
    }
}
