package com.synechisveltiosi.orderqueryservice.infrastructure;
import com.synechisveltiosi.orderqueryservice.application.ProjectionTransactions;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.*;
@Configuration
@EnableScheduling
@ConditionalOnProperty(name = "projection.rebuild.enabled", havingValue = "true", matchIfMissing = true)
public class ProjectionRebuildWorker {
    private final ProjectionTransactions projection; private final int batchSize;
    public ProjectionRebuildWorker(ProjectionTransactions projection, @Value("${projection.rebuild.batch-size:100}") int batchSize) {
        if (batchSize < 1 || batchSize > 1000) throw new IllegalArgumentException("Rebuild batch must be 1–1000");
        this.projection = projection; this.batchSize = batchSize;
    }
    @Scheduled(fixedDelayString = "${projection.rebuild.poll-delay-ms:500}")
    public void poll() {
        Long building = null;
        try { building = projection.buildingGeneration(); if (building != null) projection.rebuildStep(batchSize); }
        catch (Exception error) {
            if (building != null) projection.failRebuild(building, error.getClass().getSimpleName());
            org.slf4j.LoggerFactory.getLogger(getClass()).error("Projection rebuild stopped ({})", error.getClass().getSimpleName());
        }
    }
}
