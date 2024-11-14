package org.techbd.tasklet;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.techbd.commonvfsservice.VfsCoreService;
import org.techbd.customexception.IncompleteGroupException;
import lib.aide.vfs.VfsIngressConsumer;

import java.util.UUID;
import java.util.regex.Pattern;

/**
 * A Spring Batch tasklet implementation that processes ZIP files containing
 * demographic,
 * QE admin, and screening data. This tasklet validates and processes grouped
 * files
 * according to specific naming patterns and completeness requirements.
 */
@Component
public class ZipProcessingTasklet implements Tasklet {
    private static final Pattern FILE_PATTERN = Pattern.compile(
            "(DEMOGRAPHIC_DATA|QE_ADMIN_DATA|SCREENING)_(.+)");

    @Value("${path.inbound.folder}")
    private String inboundFolder;

    @Value("${path.ingress.home}")
    private String ingressHome;

    private final VfsCoreService vfsCoreService;

    @Autowired
    public ZipProcessingTasklet(VfsCoreService vfsCoreService) {
        this.vfsCoreService = vfsCoreService;
    }

    /**
     * Executes the tasklet, processing ZIP files in the inbound directory.
     *
     * @param contribution Mutable state to be passed back to the JobRepository
     * @param chunkContext Contains information from the job and step environment
     * @return RepeatStatus indicating whether processing should repeat
     * @throws Exception if any error occurs during processing
     */
    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        try {
            processZipFiles();
            return RepeatStatus.FINISHED;
        } catch (Exception e) {
            throw new RuntimeException("Error processing ZIP files: " + e.getMessage(), e);
        }
    }

    /**
     * Processes ZIP files from the inbound directory, validating and moving them
     * to the ingress home directory.
     *
     * @return UUID representing the processing session
     * @throws FileSystemException if there are any file system related errors
     */
    private UUID processZipFiles() throws FileSystemException {
        FileObject inboundFO = vfsCoreService.resolveFile(inboundFolder);
        FileObject ingresshomeFO = vfsCoreService.resolveFile(ingressHome);

        // Validate directories
        if (!vfsCoreService.fileExists(inboundFO)) {
            throw new FileSystemException("Inbound folder does not exist: " + inboundFO.getName().getPath());
        }
        vfsCoreService.validateAndCreateDirectories(ingresshomeFO);

        // Create VFS consumer with grouping and validation
        VfsIngressConsumer consumer = vfsCoreService.createConsumer(
                inboundFO,
                this::extractGroupId,
                this::isGroupComplete);

        return vfsCoreService.processFiles(consumer, ingresshomeFO);
    }

    /**
     * Extracts the group ID from a file name based on the defined pattern.
     *
     * @param file The file object to extract the group ID from
     * @return The extracted group ID, or null if the file name doesn't match the
     *         pattern
     */
    private String extractGroupId(FileObject file) {
        String fileName = file.getName().getBaseName();
        var matcher = FILE_PATTERN.matcher(fileName);
        return matcher.matches() ? matcher.group(2) : null;
    }

    /**
     * Validates whether a group of files is complete by checking for the presence
     * of all required file types (demographic, QE admin, and screening data).
     *
     * @param group The group of files to validate
     * @return true if the group is complete, false otherwise
     * @throws IncompleteGroupException if the group is missing required file types
     */
    private boolean isGroupComplete(VfsIngressConsumer.IngressGroup group) {
        if (group == null || group.groupedEntries().isEmpty()) {
            return false;
        }

        boolean hasDemographic = true;
        boolean hasQeAdmin = true;
        boolean hasScreening = true;

        for (VfsIngressConsumer.IngressIndividual entry : group.groupedEntries()) {
            String fileName = entry.entry().getName().getBaseName();
            if (fileName.startsWith("DEMOGRAPHIC_DATA_")) {
                hasDemographic = true;
            } else if (fileName.startsWith("QE_ADMIN_DATA_")) {
                hasQeAdmin = true;
            } else if (fileName.startsWith("SCREENING_")) {
                hasScreening = true;
            }
        }

        if (!(hasDemographic && hasQeAdmin && hasScreening)) {
            String groupId = group.groupedEntries().isEmpty() ? "unknown"
                    : extractGroupId(group.groupedEntries().get(0).entry());
            throw new IncompleteGroupException(groupId, hasDemographic, hasQeAdmin, hasScreening);
        }

        return true;
    }
}
