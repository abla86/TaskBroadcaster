using System.Collections.Concurrent;
using Microsoft.AspNetCore.SignalR;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSignalR();
builder.Services.AddSingleton<ITaskStore, InMemoryTaskStore>();
builder.Services.AddHostedService<TaskWorkerService>();

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();
app.MapHub<TaskHub>("/taskhub");

app.MapGet("/api/tasks", (ITaskStore store) =>
    Results.Ok(store.GetAll()));

app.MapGet("/api/tasks/{id:guid}", (Guid id, ITaskStore store) =>
    store.Get(id) is { } item ? Results.Ok(item) : Results.NotFound());

app.MapPost("/api/tasks", async (
    TaskRequest req,
    ITaskStore store,
    IHubContext<TaskHub> hub) =>
{
    if (string.IsNullOrWhiteSpace(req.Title))
        return Results.BadRequest(new { error = "Title cannot be empty." });

    var item = store.Add(req.Title.Trim());

    await hub.Clients.All.SendAsync("TaskCreated", item);

    return Results.Created($"/api/tasks/{item.Id}", item);
});

app.Run();

public record WorkItem(
    Guid Id,
    string Title,
    string Status,
    DateTime CreatedAt);

public record TaskRequest(string Title);

public interface ITaskStore
{
    IEnumerable<WorkItem> GetAll();
    WorkItem? Get(Guid id);
    WorkItem Add(string title);
    WorkItem? UpdateStatus(Guid id, string newStatus);
}

public class InMemoryTaskStore : ITaskStore
{
    private readonly ConcurrentDictionary<Guid, WorkItem> _items = new();

    public IEnumerable<WorkItem> GetAll() =>
        _items.Values.OrderByDescending(x => x.CreatedAt);

    public WorkItem? Get(Guid id) =>
        _items.TryGetValue(id, out var item) ? item : null;

    public WorkItem Add(string title)
    {
        var item = new WorkItem(
            Guid.NewGuid(),
            title,
            "Pending",
            DateTime.UtcNow);

        _items[item.Id] = item;
        return item;
    }

    public WorkItem? UpdateStatus(Guid id, string newStatus)
    {
        if (!_items.TryGetValue(id, out var existing))
            return null;

        var updated = existing with { Status = newStatus };
        _items[id] = updated;

        return updated;
    }
}

public class TaskHub : Hub
{
}

public class TaskWorkerService(
    ITaskStore store,
    IHubContext<TaskHub> hub,
    ILogger<TaskWorkerService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(
                    TimeSpan.FromSeconds(4),
                    stoppingToken);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                break;
            }

            var pendingTask = store
                .GetAll()
                .FirstOrDefault(t => t.Status == "Pending");

            if (pendingTask is null)
                continue;

            var updated = store.UpdateStatus(
                pendingTask.Id,
                "Completed");

            if (updated is not null)
            {
                logger.LogInformation(
                    "Jobb ferdigstilt: {Title} ({Id})",
                    updated.Title,
                    updated.Id);

                await hub.Clients.All.SendAsync(
                    "TaskUpdated",
                    updated,
                    stoppingToken);
            }
        }
    }
}
