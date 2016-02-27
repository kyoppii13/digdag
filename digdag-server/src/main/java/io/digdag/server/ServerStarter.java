package io.digdag.server;

import com.google.inject.Inject;
import javax.annotation.PostConstruct;

import io.digdag.core.database.DatabaseMigrator;
import io.digdag.core.agent.LocalAgentManager;
import io.digdag.core.schedule.ScheduleExecutor;
import io.digdag.core.session.SessionMonitorExecutor;

public class ServerStarter
{
    private final DatabaseMigrator databaseMigrator;
    private final LocalAgentManager localAgentManager;
    private final ScheduleExecutor scheduleExecutor;
    private final SessionMonitorExecutor sessionMonitorExecutor;

    @Inject
    public ServerStarter(
            DatabaseMigrator databaseMigrator,
            LocalAgentManager localAgentManager,
            ScheduleExecutor scheduleExecutor,
            SessionMonitorExecutor sessionMonitorExecutor)
    {
        this.databaseMigrator = databaseMigrator;
        this.localAgentManager = localAgentManager;
        this.scheduleExecutor = scheduleExecutor;
        this.sessionMonitorExecutor = sessionMonitorExecutor;
    }

    @PostConstruct
    public void start()
    {
        databaseMigrator.migrate();
    }
}
