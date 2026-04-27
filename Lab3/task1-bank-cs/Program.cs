using System.Text;

namespace BankLab;

internal static class Program
{
    private static int Main(string[] args)
    {
        Console.OutputEncoding = Encoding.UTF8;

        var options = SimulationOptions.FromArgs(args);

        EnsureDirectoryForFile(options.CsvPath);

        Console.WriteLine("=== Лабораторна робота №3 / Задача 1 ===");
        PrintScenario(options);
        Console.WriteLine($"CSV: {options.CsvPath}");
        Console.WriteLine();

        SimulationResult result;

        if (options.Mode == TransferMode.DeadlockDemo)
        {
            result = DeadlockDemo.Run(options);
        }
        else
        {
            var simulator = new BankSimulator(options);
            result = simulator.Run();
        }

        CsvLogger.Append(result, options.CsvPath);
        PrintResult(result);

        return 0;
    }

    private static void EnsureDirectoryForFile(string filePath)
    {
        var directory = Path.GetDirectoryName(filePath);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }
    }

    private static void PrintScenario(SimulationOptions options)
    {
        Console.WriteLine($"Режим: {options.Mode}");

        if (options.Mode == TransferMode.DeadlockDemo)
        {
            Console.WriteLine("Сценарій: демонстраційний deadlock із двома рахунками.");
            Console.WriteLine("Рахунків: 2");
            Console.WriteLine("Потоків: 2");
            Console.WriteLine("Загальна кількість переказів: 2");
            Console.WriteLine($"Таймаут виявлення deadlock: {options.DeadlockTimeoutMs} ms");
            return;
        }

        Console.WriteLine($"Рахунків: {options.AccountsCount}");
        Console.WriteLine($"Потоків: {options.ThreadsCount}");
        Console.WriteLine($"Загальна кількість переказів: {options.TotalTransfers}");
        Console.WriteLine($"Базово на потік: {options.TotalTransfers / Math.Max(1, options.ThreadsCount)}");
    }

    private static void PrintResult(SimulationResult result)
    {
        Console.WriteLine("=== РЕЗУЛЬТАТ ===");
        Console.WriteLine($"Mode: {result.Mode}");
        Console.WriteLine($"AccountsCount: {result.AccountsCount}");
        Console.WriteLine($"ThreadsCount: {result.ThreadsCount}");
        Console.WriteLine($"TotalTransfers: {result.TotalTransfers}");
        Console.WriteLine($"ElapsedMs: {result.ElapsedMs}");
        Console.WriteLine($"InitialTotal: {result.InitialTotal}");
        Console.WriteLine($"FinalTotal: {result.FinalTotal}");
        Console.WriteLine($"InvariantPreserved: {result.InvariantPreserved}");
        Console.WriteLine($"NegativeAccounts: {result.NegativeAccounts}");
        Console.WriteLine($"DeadlockDetected: {result.DeadlockDetected}");
        Console.WriteLine($"Notes: {result.Notes}");
        Console.WriteLine();

        if (result.InvariantPreserved && !result.DeadlockDetected)
        {
            Console.WriteLine("Висновок: інваріант збережено, проблем не виявлено.");
        }
        else
        {
            Console.WriteLine("Висновок: продемонстровано проблему паралельного доступу або deadlock.");
        }
    }
}
