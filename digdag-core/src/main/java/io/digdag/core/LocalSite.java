package io.digdag.core;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.time.Instant;
import java.time.ZoneId;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.SchedulerManager;
import io.digdag.core.session.*;
import io.digdag.core.workflow.*;
import io.digdag.core.workflow.TaskMatchPattern.MultipleTaskMatchException;
import io.digdag.core.workflow.TaskMatchPattern.NoMatchException;
import io.digdag.spi.Scheduler;
import io.digdag.spi.ScheduleTime;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;

public class LocalSite
{
    private static Logger logger = LoggerFactory.getLogger(LocalSite.class);

    private final WorkflowCompiler compiler;
    private final RepositoryStore repoStore;
    private final SessionStore sessionStore;
    private final AttemptBuilder attemptBuilder;
    private final WorkflowExecutor exec;
    private final SchedulerManager srm;

    @Inject
    public LocalSite(
            WorkflowCompiler compiler,
            RepositoryStoreManager repoStoreManager,
            SessionStoreManager sessionStoreManager,
            AttemptBuilder attemptBuilder,
            WorkflowExecutor exec,
            SchedulerManager srm)
    {
        this.compiler = compiler;
        this.repoStore = repoStoreManager.getRepositoryStore(0);
        this.sessionStore = sessionStoreManager.getSessionStore(0);
        this.attemptBuilder = attemptBuilder;
        this.exec = exec;
        this.srm = srm;
    }

    public SessionStore getSessionStore()
    {
        return sessionStore;
    }

    private class StoreWorkflowResult
    {
        private final StoredRevision revision;
        private final List<StoredWorkflowDefinition> workflows;

        public StoreWorkflowResult(StoredRevision revision, List<StoredWorkflowDefinition> workflows)
        {
            this.revision = revision;
            this.workflows = workflows;
        }

        public StoredRevision getRevision()
        {
            return revision;
        }

        public List<StoredWorkflowDefinition> getWorkflows()
        {
            return workflows;
        }
    }

    private StoreWorkflowResult storeLocalWorkflowsImpl(
            String repositoryName, Revision revision,
            WorkflowDefinitionList defs,
            Optional<Instant> currentTimeToSchedule)
        throws ResourceConflictException, ResourceNotFoundException
    {
        // validate workflow
        // TODO move this to RepositoryControl
        defs.get()
            .stream()
            .forEach(workflowSource -> compiler.compile(workflowSource.getName(), workflowSource.getConfig()));

        return repoStore.putAndLockRepository(
                Repository.of(repositoryName),
                (store, storedRepo) -> {
                    RepositoryControl lockedRepo = new RepositoryControl(store, storedRepo);
                    StoredRevision rev = lockedRepo.putRevision(revision);
                    List<StoredWorkflowDefinition> storedDefs;
                    if (currentTimeToSchedule.isPresent()) {
                        storedDefs = lockedRepo.insertWorkflowDefinitions(rev, defs.get(), srm, currentTimeToSchedule.get());
                    }
                    else {
                        storedDefs = lockedRepo.insertWorkflowDefinitionsWithoutSchedules(rev, defs.get());
                    }
                    return new StoreWorkflowResult(rev, storedDefs);
                });
    }

    private StoreWorkflowResult storeLocalWorkflows(
            String revisionName, ArchiveMetadata archive,
            Optional<Instant> currentTimeToSchedule)
        throws ResourceConflictException, ResourceNotFoundException
    {
        return storeLocalWorkflowsImpl(
                "default",
                Revision.builderFromArchive(revisionName, archive)
                    .archiveType("null")
                    .build(),
                archive.getWorkflowList(),
                currentTimeToSchedule);
    }

    public StoredRevision storeWorkflows(
            String revisionName,
            ArchiveMetadata archive,
            Instant currentTime)
        throws ResourceConflictException, ResourceNotFoundException
    {
        return storeLocalWorkflows(revisionName, archive, Optional.of(currentTime))
            .getRevision();
    }

    public interface SessionTimeSupplier
    {
        ScheduleTime get(Optional<Scheduler> sr, ZoneId timeZone);
    }

    public StoredSessionAttemptWithSession storeAndStartLocalWorkflows(
            ArchiveMetadata archive,
            TaskMatchPattern taskMatchPattern,
            Config overwriteParams,
            SessionTimeSupplier supplier)
        throws ResourceConflictException, ResourceNotFoundException, SessionAttemptConflictException
    {
        StoreWorkflowResult revWfs = storeLocalWorkflows("revision", archive, Optional.absent());

        StoredRevision rev = revWfs.getRevision();
        List<StoredWorkflowDefinition> sources = revWfs.getWorkflows();

        try {
            StoredWorkflowDefinition def = taskMatchPattern.findRootWorkflow(sources);

            Optional<Scheduler> sr = srm.tryGetScheduler(rev, def);
            ScheduleTime sessionTime = supplier.get(sr, sr.transform(it -> it.getTimeZone()).or(archive.getDefaultTimeZone()));

            AttemptRequest ar = attemptBuilder.buildFromStoredWorkflow(
                    Optional.absent(),
                    rev,
                    def,
                    overwriteParams,
                    sessionTime);

            if (taskMatchPattern.getSubtaskMatchPattern().isPresent()) {
                return exec.submitSubworkflow(0, ar, def, taskMatchPattern.getSubtaskMatchPattern().get());
            }
            else {
                return exec.submitWorkflow(0, ar, def);
            }
        }
        catch (NoMatchException ex) {
            //logger.error("No task matched with '{}'", fromTaskName.orNull());
            throw new IllegalArgumentException(ex);  // TODO exception class
        }
        catch (MultipleTaskMatchException ex) {
            throw new IllegalArgumentException(ex);  // TODO exception class
        }
    }

    public void run()
            throws InterruptedException
    {
        exec.run();
    }

    public void runUntilAny()
            throws InterruptedException
    {
        exec.runUntilAny();
    }
}
