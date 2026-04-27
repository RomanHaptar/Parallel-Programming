using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Versioning;
using System.Text;
using System.Text.Json;

namespace CsHelper;

internal static class Program
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private static int Main(string[] args)
    {
        var options = HelperOptions.FromArgs(args);

        Directory.CreateDirectory(options.WorkDir);
        EnsureDirectoryForFile(options.LogPath);

        try
        {
            return options.Mode.ToLowerInvariant() switch
            {
                "tcp" => RunTcp(options),
                "file" => RunFile(options),
                "mmap" => RunMmapGuarded(options),
                _ => Fail($"Невідомий режим: {options.Mode}")
            };
        }
        catch (Exception ex)
        {
            Console.WriteLine($"ERROR: {ex.Message}");
            AppendLog(options.LogPath, $"ERROR: {ex}");
            return 1;
        }
    }

    private static int RunTcp(HelperOptions options)
    {
        using var listener = new TcpListener(IPAddress.Loopback, options.Port);
        listener.Start();

        Console.WriteLine("READY:tcp");
        AppendLog(options.LogPath, $"TCP helper started on port {options.Port}");

        using var client = listener.AcceptTcpClient();
        client.NoDelay = true;

        using var stream = client.GetStream();
        using var reader = new StreamReader(stream, Encoding.UTF8);
        using var writer = new StreamWriter(stream, new UTF8Encoding(false)) { AutoFlush = true };

        while (true)
        {
            string? line = reader.ReadLine();
            if (line is null)
            {
                break;
            }

            var request = JsonSerializer.Deserialize<IpcRequest>(line, JsonOptions);
            if (request is null)
            {
                continue;
            }

            if (request.Terminate)
            {
                writer.WriteLine(JsonSerializer.Serialize(new IpcResponse(request.Seq, request.Value, true, "bye"), JsonOptions));
                break;
            }

            AppendLog(options.LogPath, $"TCP seq={request.Seq} value={request.Value}");

            var response = new IpcResponse(request.Seq, request.Value, true, "ok");
            writer.WriteLine(JsonSerializer.Serialize(response, JsonOptions));
        }

        AppendLog(options.LogPath, "TCP helper stopped");
        return 0;
    }

    private static int RunFile(HelperOptions options)
    {
        string requestPath = Path.Combine(options.WorkDir, "request.json");
        string responsePath = Path.Combine(options.WorkDir, "response.json");

        Console.WriteLine("READY:file");
        AppendLog(options.LogPath, $"File helper started in {options.WorkDir}");

        int lastProcessedSeq = -1;
        var idleStopwatch = Stopwatch.StartNew();

        while (true)
        {
            if (File.Exists(requestPath))
            {
                try
                {
                    string json = SafeReadAllText(requestPath);
                    if (!string.IsNullOrWhiteSpace(json))
                    {
                        var request = JsonSerializer.Deserialize<IpcRequest>(json, JsonOptions);

                        if (request is not null)
                        {
                            if (request.Terminate)
                            {
                                AppendLog(options.LogPath, "File helper received terminate");
                                break;
                            }

                            if (request.Seq != lastProcessedSeq)
                            {
                                AppendLog(options.LogPath, $"FILE seq={request.Seq} value={request.Value}");

                                var response = new IpcResponse(request.Seq, request.Value, true, "ok");
                                SafeWriteAllText(responsePath, JsonSerializer.Serialize(response, JsonOptions));

                                lastProcessedSeq = request.Seq;
                                idleStopwatch.Restart();
                            }
                        }
                    }
                }
                catch
                {
                    // Файл може бути ще в процесі запису — просто повторимо цикл.
                }
            }

            if (idleStopwatch.ElapsedMilliseconds > options.IdleTimeoutMs)
            {
                AppendLog(options.LogPath, "File helper idle timeout");
                break;
            }

            Thread.Sleep(1);
        }

        AppendLog(options.LogPath, "File helper stopped");
        return 0;
    }

    private static int RunMmapGuarded(HelperOptions options)
    {
        if (!OperatingSystem.IsWindows())
        {
            return Fail("mmap mode підтримується лише на Windows у цій реалізації.");
        }

        return RunMmapWindows(options);
    }

    [SupportedOSPlatform("windows")]
    private static int RunMmapWindows(HelperOptions options)
    {
        using var mmf = MemoryMappedFile.CreateOrOpen(options.MapName, options.MmapSize, MemoryMappedFileAccess.ReadWrite);
        using var accessor = mmf.CreateViewAccessor(0, options.MmapSize, MemoryMappedFileAccess.ReadWrite);
        using var requestEvent = new EventWaitHandle(false, EventResetMode.AutoReset, options.MapName + "_REQ");
        using var responseEvent = new EventWaitHandle(false, EventResetMode.AutoReset, options.MapName + "_RESP");

        accessor.Write(0, 0);   // terminate flag
        accessor.Write(4, 0);   // seq
        accessor.Write(8, 0);   // request value
        accessor.Write(12, 0);  // response value

        Console.WriteLine("READY:mmap");
        AppendLog(options.LogPath, $"MMAP helper started: {options.MapName} (event-based)");

        while (true)
        {
            bool signaled = requestEvent.WaitOne(options.IdleTimeoutMs);
            if (!signaled)
            {
                AppendLog(options.LogPath, "MMAP helper idle timeout");
                break;
            }

            int terminate = accessor.ReadInt32(0);
            int seq = accessor.ReadInt32(4);
            int value = accessor.ReadInt32(8);

            if (terminate == 1)
            {
                AppendLog(options.LogPath, "MMAP helper received terminate");
                break;
            }

            AppendLog(options.LogPath, $"MMAP seq={seq} value={value}");

            accessor.Write(12, value);
            responseEvent.Set();
        }

        AppendLog(options.LogPath, "MMAP helper stopped");
        return 0;
    }

    private static void EnsureDirectoryForFile(string filePath)
    {
        string? directory = Path.GetDirectoryName(filePath);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }
    }

    private static void AppendLog(string logPath, string message)
    {
        EnsureDirectoryForFile(logPath);
        File.AppendAllText(logPath, $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] {message}{Environment.NewLine}");
    }

    private static string SafeReadAllText(string path)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var reader = new StreamReader(fs, Encoding.UTF8);
        return reader.ReadToEnd();
    }

    private static void SafeWriteAllText(string path, string content)
    {
        string tempPath = path + ".tmp";
        File.WriteAllText(tempPath, content, new UTF8Encoding(false));
        File.Move(tempPath, path, overwrite: true);
    }

    private static int Fail(string message)
    {
        Console.WriteLine($"ERROR: {message}");
        return 1;
    }
}

internal sealed class HelperOptions
{
    public string Mode { get; init; } = "tcp";
    public int Port { get; init; } = 5001;
    public string WorkDir { get; init; } = Path.Combine(".", "ipc_runtime");
    public string MapName { get; init; } = "Lab3IpcMap";
    public int MmapSize { get; init; } = 64;
    public int IdleTimeoutMs { get; init; } = 30000;
    public string LogPath { get; init; } = Path.Combine(".", "ipc_runtime", "cs_helper.log");

    public static HelperOptions FromArgs(string[] args)
    {
        var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var arg in args)
        {
            var parts = arg.Split('=', 2, StringSplitOptions.TrimEntries);
            if (parts.Length == 2)
            {
                dict[parts[0]] = parts[1];
            }
        }

        return new HelperOptions
        {
            Mode = Get(dict, "mode", "tcp"),
            Port = Math.Max(1, ParseInt(Get(dict, "port", "5001"), 5001)),
            WorkDir = Get(dict, "workdir", Path.Combine(".", "ipc_runtime")),
            MapName = Get(dict, "mapName", "Lab3IpcMap"),
            MmapSize = Math.Max(32, ParseInt(Get(dict, "mmapSize", "64"), 64)),
            IdleTimeoutMs = Math.Max(1000, ParseInt(Get(dict, "idleTimeoutMs", "30000"), 30000)),
            LogPath = Get(dict, "log", Path.Combine(".", "ipc_runtime", "cs_helper.log"))
        };
    }

    private static string Get(Dictionary<string, string> dict, string key, string defaultValue)
        => dict.TryGetValue(key, out var value) ? value : defaultValue;

    private static int ParseInt(string value, int fallback)
        => int.TryParse(value, out var parsed) ? parsed : fallback;
}

internal sealed record IpcRequest(int Seq, int Value, bool Terminate = false);

internal sealed record IpcResponse(int Seq, int Value, bool Logged = true, string Status = "ok");
