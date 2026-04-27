namespace BankLab;

public static class CsvLogger
{
    public static void Append(SimulationResult result, string csvPath)
    {
        bool fileExists = File.Exists(csvPath);

        using var writer = new StreamWriter(csvPath, append: true);

        if (!fileExists)
        {
            writer.WriteLine(
                "TimestampUtc,Mode,AccountsCount,ThreadsCount,TotalTransfers,InitialTotal,FinalTotal,ElapsedMs,InvariantPreserved,NegativeAccounts,DeadlockDetected,Notes");
        }

        writer.WriteLine(string.Join(",",
            Escape(result.TimestampUtc.ToString("O")),
            Escape(result.Mode),
            result.AccountsCount,
            result.ThreadsCount,
            result.TotalTransfers,
            result.InitialTotal,
            result.FinalTotal,
            result.ElapsedMs,
            result.InvariantPreserved,
            result.NegativeAccounts,
            result.DeadlockDetected,
            Escape(result.Notes)
        ));
    }

    private static string Escape(string value)
    {
        value ??= string.Empty;
        return $"\"{value.Replace("\"", "\"\"")}\"";
    }
}
