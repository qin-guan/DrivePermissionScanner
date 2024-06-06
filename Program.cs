using Google.Apis.Auth.OAuth2;
using Google.Apis.Drive.v3;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;
using ConsoleAppFramework;
using Google.Apis.Services;
using File = Google.Apis.Drive.v3.Data.File;

var app = ConsoleApp.Create();
app.Add<Commands>();
app.Run(args);

internal class Commands
{
    /// <summary>
    /// Generates a recursive JSON tree of the items under the root file.
    /// </summary>
    /// <param name="rootFileId">ID of the root folder.</param>
    /// <param name="clientSecretPath">Path of the Google Cloud API client secret.</param>
    /// <param name="user">User to authorize as.</param>
    /// <param name="applicationName">Application name.</param>
    /// <param name="jsonOutputPath">Path to output JSON tree.</param>
    [Command("start")]
    public async Task Start(
        string rootFileId,
        string clientSecretPath = "client_secrets.json",
        string user = "ogp",
        string applicationName = "DriverPermissionScanner",
        string jsonOutputPath = "output.json"
    )
    {
        await using var stream = new FileStream(clientSecretPath, FileMode.Open, FileAccess.Read);
        var googleClientSecrets = await GoogleClientSecrets.FromStreamAsync(stream);

        var credential = await GoogleWebAuthorizationBroker.AuthorizeAsync(
            googleClientSecrets.Secrets,
            new[] { DriveService.Scope.DriveReadonly },
            user,
            CancellationToken.None
        );

        var service = new DriveService(
            new BaseClientService.Initializer
            {
                HttpClientInitializer = credential,
                ApplicationName = applicationName
            }
        );

        const int maxDegreeOfParallelism = 1500;
        var sem = new SemaphoreSlim(maxDegreeOfParallelism);

        var fetchChildren = new TransformBlock<Node, Node>(async (node) =>
        {
            await sem.WaitAsync();

            var token = "";
            do
            {
                var query = service.Files.List();
                query.Q = $"'{node.File.Id}' in parents and trashed = false";
                query.Fields = "*";
                query.PageToken ??= token;

                var files = await query.ExecuteAsync();
                token = files.NextPageToken;

                node.Children.AddRange(files.Files.Select(f => new Node(f)));
            } while (!string.IsNullOrWhiteSpace(token));

            return node;
        }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism });

        var recurse = new ActionBlock<Node>(node =>
        {
            sem.Release();

            var children = node.Children.Where(c => c.IsFolder).ToArray();

            Console.WriteLine(
                "Children: {1}\t\tIn flight: {2}\t\tPipeline input count: {3}\t\tFile: {0}",
                node.File.Name, children.Length, maxDegreeOfParallelism - sem.CurrentCount, fetchChildren.InputCount
            );

            foreach (var child in children)
            {
                fetchChildren.Post(child);
            }

            if (
                fetchChildren.InputCount != 0 ||
                children.Length != 0 ||
                sem.CurrentCount != maxDegreeOfParallelism
            ) return;

            Console.WriteLine("Calling completion with file name: {0}", node.File.Name);

            fetchChildren.Complete();
        });

        fetchChildren.LinkTo(recurse, new DataflowLinkOptions { PropagateCompletion = true });

        var root = await service.Files.Get(rootFileId).ExecuteAsync();
        var rootNode = new Node(root);

        fetchChildren.Post(rootNode);
        await recurse.Completion;

        var jsonFileStream = new FileStream(jsonOutputPath, FileMode.Create, FileAccess.ReadWrite);

        await JsonSerializer.SerializeAsync(
            jsonFileStream,
            rootNode,
            new JsonSerializerOptions { WriteIndented = true }
        );
    }

    [Command("stats")]
    public async Task Stats(string jsonInputPath = "output.json")
    {
        await using var stream = new FileStream(jsonInputPath, FileMode.Open, FileAccess.Read);
        var root = await JsonSerializer.DeserializeAsync<NodeWithPath>(stream);
        if (root is null) throw new Exception("Root is null.");

        var options = new DataflowLinkOptions
        {
            PropagateCompletion = true
        };

        var blockOptions = new ExecutionDataflowBlockOptions
        {
            MaxDegreeOfParallelism = 1000
        };

        var transformer = new TransformBlock<NodeWithPath, NodeWithPath>(node =>
        {
            foreach (var child in node.Children)
            {
                child.Path.AddRange(node.Path);
                child.Path.Add(child.File.Name);
            }

            return node;
        }, blockOptions);

        var sharedWithAnyone = new TransformBlock<NodeWithPath, NodeWithPath?>(node =>
        {
            if (node.File.Permissions is null) return null;
            return node.File.Permissions.Any(p => p.Type is not null && p.Type == "anyone") ? node : null;
        }, blockOptions);

        var counter = new
        {
            Folders = new List<NodeWithPath>(),
            Files = new List<NodeWithPath>(),
        };

        var logger = new ActionBlock<NodeWithPath>(node =>
        {
            if (node.IsFolder)
            {
                Console.WriteLine(string.Join('/', node.Path));
                counter.Folders.Add(node);
            }
            else
            {
                counter.Files.Add(node);
            }
        }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });

        transformer.LinkTo(sharedWithAnyone, options);

        sharedWithAnyone.LinkTo(
            logger!,
            options,
            node => node is not null
        );
        
        sharedWithAnyone.LinkTo(DataflowBlock.NullTarget<NodeWithPath>()!);

        var queue = new Queue<NodeWithPath>();
        queue.Enqueue(root);

        while (queue.Count > 0)
        {
            var node = queue.Dequeue();
            transformer.Post(node);

            foreach (var child in node.Children)
            {
                queue.Enqueue(child);
            }
        }

        transformer.Complete();
        await logger.Completion;

        return;
    }
}

internal record Node(File File)
{
    public List<Node> Children { get; init; } = [];
    public bool IsFolder => File.MimeType == "application/vnd.google-apps.folder";
}

internal record NodeWithPath(File File)
{
    public List<string> Path { get; init; } = [];
    public List<NodeWithPath> Children { get; init; } = [];
    public bool IsFolder => File.MimeType == "application/vnd.google-apps.folder";
}