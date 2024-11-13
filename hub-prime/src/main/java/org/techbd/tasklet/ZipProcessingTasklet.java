package org.techbd.tasklet;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.techbd.customexception.IncompleteGroupException;

import jakarta.annotation.PostConstruct;
import lib.aide.vfs.VfsIngressConsumer;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.VFS;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.vfs2.FileSystemException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tasklet implementation for processing ZIP files in a batch step.
 * This class validates file structure, groups files based on specific patterns,
 * and audits the processing results.
 */
@Component
public class ZipProcessingTasklet implements Tasklet {
    private static final Logger logger = LoggerFactory.getLogger(ZipProcessingTasklet.class);

    // Regular expressions for file patterns
    private static final Pattern FILE_PATTERN = Pattern.compile(
            "(DEMOGRAPHIC_DATA|QE_ADMIN_DATA|SCREENING)_(.+)");

    @Value("${path.inbound.folder}")
    private String inboundFolder;

    @Value("${path.ingress.home}")
    private String ingressHome;

    @PostConstruct
    public void init() {
        System.out.println("Resolved Inbound Folder Path: " + inboundFolder);
        System.out.println("Resolved Ingress Home Path: " + ingressHome);
    }

    private String extractGroupId(FileObject file) {
        String fileName = file.getName().getBaseName();
        Matcher matcher = FILE_PATTERN.matcher(fileName);

        if (matcher.matches()) {
            // Extract the common suffix (group identifier)
            return matcher.group(2);
        }
        return null;
    }

    private boolean isGroupComplete(VfsIngressConsumer.IngressGroup group) {
        if (group == null || group.groupedEntries().isEmpty()) {
            return false;
        }

        // Check if we have all required file types in the group
        boolean hasDemographic = true;
        boolean hasQeAdmin = true;
        // TODO: Change the value later, this is for testing only for custom Exception
        boolean hasScreening = false;

        for (VfsIngressConsumer.IngressIndividual entry : group.groupedEntries()) {
            String fileName = entry.entry().getName().getBaseName();
            if (fileName.startsWith("DEMOGRAPHIC_DATA_")) {
                hasDemographic = true;
            } else if (fileName.startsWith("QE_ADMIN_DATA_")) {
                hasQeAdmin = true;
            } else if (fileName.startsWith("SCREENING_")) {
                // TODO: Change the value later, this is for testing only
                hasScreening = false;
            }
        }

        if (!(hasDemographic && hasQeAdmin && hasScreening)) {
            String groupId = group.groupedEntries().isEmpty() ? "unknown"
                    : extractGroupId(group.groupedEntries().get(0).entry());
            throw new IncompleteGroupException(groupId, hasDemographic, hasQeAdmin, hasScreening);
        }

        return true;
    }

    private void validateFolderStructure(FileObject inboundFO, FileObject ingresshomeFO) throws FileSystemException {
        if (!inboundFO.exists()) {
            throw new FileSystemException("Inbound folder does not exist: " + inboundFO.getName().getPath());
        }

        if (!ingresshomeFO.exists()) {
            logger.info("Creating ingress home folder: {}", ingresshomeFO.getName().getPath());
            ingresshomeFO.createFolder();
        }

    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        logger.info("Starting ZIP file processing tasklet");

        FileObject inboundFO = VFS.getManager().resolveFile(inboundFolder);
        FileObject ingresshomeFO = VFS.getManager().resolveFile(ingressHome);
        validateFolderStructure(inboundFO, ingresshomeFO);

        lib.aide.vfs.VfsIngressConsumer consumer = new lib.aide.vfs.VfsIngressConsumer.Builder()
                .addIngressPath(inboundFO)
                .isGroup(this::extractGroupId) // Group files based on common suffix
                .isGroupComplete(this::isGroupComplete) // Check if group has all required files
                .isSnapshotable((ingressEntry, file, dir, audit) -> {
                    String extension = ingressEntry.entry().getName().getExtension();
                    return extension.equalsIgnoreCase("zip");
                })
                .populateSnapshot((ingressEntry, file, dir, audit) -> List.of())
                .consumables(lib.aide.vfs.VfsIngressConsumer::consumeUnzipped)
                .build();

        UUID sessionId = UUID.randomUUID();
        consumer.drain(ingresshomeFO, Optional.of(sessionId));

        logger.info("ZIP processing completed successfully. Session ID: {}", sessionId);

        // Log detailed audit events
        consumer.getAudit().events().forEach(event -> {
            if (event.path().isPresent()) {
                logger.info("Audit event: {} - {} - {}",
                        event.nature(),
                        event.message(),
                        event.path().get().getName().getBaseName());
            } else {
                logger.info("Audit event: {} - {}",
                        event.nature(),
                        event.message());
            }
        });

        return RepeatStatus.FINISHED;
    }
}
