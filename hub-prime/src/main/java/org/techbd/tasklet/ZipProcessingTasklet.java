package org.techbd.tasklet;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.VFS;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Component
public class ZipProcessingTasklet implements Tasklet {
    private static final Logger logger = LoggerFactory.getLogger(ZipProcessingTasklet.class);
    @Value("${path.inbound.folder}")
    private String inboundFolder;

    @Value("${path.ingress.home}")
    private String ingressHome;

    @PostConstruct
    public void init() {
        System.out.println("Resolved Inbound Folder Path: " + inboundFolder);
        System.out.println("Resolved Ingress Home Path: " + ingressHome);
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        logger.info("Starting ZIP file processing tasklet");

        try {
            FileObject inboundFO = VFS.getManager().resolveFile(inboundFolder);
            FileObject ingresshomeFO = VFS.getManager().resolveFile(ingressHome);

            if (!ingresshomeFO.exists()) {
                ingresshomeFO.createFolder();
            }

            lib.aide.vfs.VfsIngressConsumer consumer = new lib.aide.vfs.VfsIngressConsumer.Builder()
                    .addIngressPath(inboundFO)
                    .isGroup(file -> null) // No grouping needed
                    .isGroupComplete(group -> true) // Not using groups
                    .isSnapshotable((ingressEntry, file, dir, audit) -> ingressEntry.entry().getName().getExtension()
                            .equalsIgnoreCase("zip"))
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

        } catch (Exception e) {
            logger.error("Error processing ZIP files", e);
            throw e;
        }
        return RepeatStatus.FINISHED;
    }
}
