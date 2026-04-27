using System.Diagnostics;

namespace BankLab;

public enum TransferMode
{
    Sequential,
    Unsafe,
    Safe,
    DeadlockDemo
}

public sealed class SimulationOptions
{
    public TransferMode Mode { get; init; } = TransferMode.Safe;
    public int AccountsCount { get; init; } = 200;
    public int ThreadsCount { get; init; } = 1000;
    public int TotalTransfers { get; init; } = 100000;
    public int MinInitialBalance { get; init; } = 1000;
    public int MaxInitialBalance { get; init; } = 10000;
    public int MaxTransferAmount { get; init; } = 200;
    public int Seed { get; init; } = 42;
    public int DeadlockTimeoutMs { get; init; } = 2000;
    public string CsvPath { get; init; } = Path.Combine("..", "results", "task1_results.csv");

    public static SimulationOptions FromArgs(string[] args)
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

        int threads = Math.Max(1, ParseInt(Get(dict, "threads", "1000"), 1000));
        int accounts = Math.Max(2, ParseInt(Get(dict, "accounts", "200"), 200));
        int totalTransfers = Math.Max(1, ParseInt(Get(dict, "totalTransfers", "100000"), 100000));

        return new SimulationOptions
        {
            Mode = ParseMode(Get(dict, "mode", "safe")),
            AccountsCount = accounts,
            ThreadsCount = threads,
            TotalTransfers = totalTransfers,
            MinInitialBalance = Math.Max(1, ParseInt(Get(dict, "minBalance", "1000"), 1000)),
            MaxInitialBalance = Math.Max(1, ParseInt(Get(dict, "maxBalance", "10000"), 10000)),
            MaxTransferAmount = Math.Max(1, ParseInt(Get(dict, "maxTransfer", "200"), 200)),
            Seed = ParseInt(Get(dict, "seed", "42"), 42),
            DeadlockTimeoutMs = Math.Max(100, ParseInt(Get(dict, "deadlockTimeoutMs", "2000"), 2000)),
            CsvPath = Get(dict, "csv", Path.Combine("..", "results", "task1_results.csv"))
        };
    }

    private static string Get(Dictionary<string, string> dict, string key, string defaultValue)
        => dict.TryGetValue(key, out var value) ? value : defaultValue;

    private static int ParseInt(string value, int fallback)
        => int.TryParse(value, out var parsed) ? parsed : fallback;

    private static TransferMode ParseMode(string value)
        => value.Trim().ToLowerInvariant() switch
        {
            "sequential" => TransferMode.Sequential,
            "unsafe" => TransferMode.Unsafe,
            "safe" => TransferMode.Safe,
            "deadlock" => TransferMode.DeadlockDemo,
            "deadlock-demo" => TransferMode.DeadlockDemo,
            _ => TransferMode.Safe
        };
}

public sealed class SimulationResult
{
    public string Mode { get; init; } = "";
    public int AccountsCount { get; init; }
    public int ThreadsCount { get; init; }
    public int TotalTransfers { get; init; }
    public long InitialTotal { get; init; }
    public long FinalTotal { get; init; }
    public long ElapsedMs { get; init; }
    public bool InvariantPreserved { get; init; }
    public int NegativeAccounts { get; init; }
    public bool DeadlockDetected { get; init; }
    public string Notes { get; init; } = "";
    public DateTime TimestampUtc { get; init; } = DateTime.UtcNow;
}

public sealed class Account
{
    public int Id { get; }
    public int Balance;
    public object SyncRoot { get; } = new();

    public Account(int id, int balance)
    {
        Id = id;
        Balance = balance;
    }
}

public sealed class BankSimulator
{
    private readonly SimulationOptions _options;
    private readonly Account[] _accounts;

    public BankSimulator(SimulationOptions options)
    {
        _options = options;

        var rng = new Random(_options.Seed);
        _accounts = Enumerable.Range(0, _options.AccountsCount)
            .Select(i => new Account(i, rng.Next(_options.MinInitialBalance, _options.MaxInitialBalance + 1)))
            .ToArray();
    }

    public SimulationResult Run()
    {
        return _options.Mode switch
        {
            TransferMode.Sequential => RunSequential(),
            TransferMode.Unsafe => RunConcurrent(UnsafeTransfer),
            TransferMode.Safe => RunConcurrent(SafeTransfer),
            _ => throw new InvalidOperationException("Непідтримуваний режим для BankSimulator.")
        };
    }

    private SimulationResult RunSequential()
    {
        long initialTotal = TotalBalance();
        var stopwatch = Stopwatch.StartNew();

        var rng = new Random(_options.Seed + 999);

        for (int i = 0; i < _options.TotalTransfers; i++)
        {
            var (from, to, amount) = PickTransfer(rng);
            SequentialTransfer(from, to, amount);
        }

        stopwatch.Stop();
        long finalTotal = TotalBalance();
        int negativeAccounts = CountNegativeAccounts();

        return BuildResult(
            mode: TransferMode.Sequential,
            initialTotal: initialTotal,
            finalTotal: finalTotal,
            elapsedMs: stopwatch.ElapsedMilliseconds,
            negativeAccounts: negativeAccounts,
            deadlockDetected: false,
            notes: "Послідовний базовий режим без паралелізації."
        );
    }

    private SimulationResult RunConcurrent(Action<Account, Account, int> transferAction)
    {
        long initialTotal = TotalBalance();
        var stopwatch = Stopwatch.StartNew();

        var threadLocalRandom = new ThreadLocal<Random>(() =>
            new Random(unchecked(Environment.TickCount * 31 + Thread.CurrentThread.ManagedThreadId)));

        var threads = new Thread[_options.ThreadsCount];

        int baseTransfersPerThread = _options.TotalTransfers / _options.ThreadsCount;
        int remainder = _options.TotalTransfers % _options.ThreadsCount;

        for (int i = 0; i < _options.ThreadsCount; i++)
        {
            int threadIndex = i;
            int transfersForThisThread = baseTransfersPerThread + (threadIndex < remainder ? 1 : 0);

            threads[i] = new Thread(() =>
            {
                var rng = threadLocalRandom.Value!;

                for (int j = 0; j < transfersForThisThread; j++)
                {
                    var (from, to, amount) = PickTransfer(rng);
                    transferAction(from, to, amount);
                }
            })
            {
                IsBackground = false
            };
        }

        foreach (var thread in threads)
        {
            thread.Start();
        }

        foreach (var thread in threads)
        {
            thread.Join();
        }

        stopwatch.Stop();
        long finalTotal = TotalBalance();
        int negativeAccounts = CountNegativeAccounts();

        string notes = _options.Mode switch
        {
            TransferMode.Unsafe => "Навмисно відсутня синхронізація; можливі race condition, втрата оновлень, від’ємний баланс.",
            TransferMode.Safe => "Використано lock і глобальний порядок захоплення двох рахунків за Id.",
            _ => ""
        };

        return BuildResult(
            mode: _options.Mode,
            initialTotal: initialTotal,
            finalTotal: finalTotal,
            elapsedMs: stopwatch.ElapsedMilliseconds,
            negativeAccounts: negativeAccounts,
            deadlockDetected: false,
            notes: notes
        );
    }

    private SimulationResult BuildResult(
        TransferMode mode,
        long initialTotal,
        long finalTotal,
        long elapsedMs,
        int negativeAccounts,
        bool deadlockDetected,
        string notes)
    {
        bool invariantPreserved = initialTotal == finalTotal && negativeAccounts == 0 && !deadlockDetected;

        return new SimulationResult
        {
            Mode = mode.ToString(),
            AccountsCount = _options.AccountsCount,
            ThreadsCount = _options.ThreadsCount,
            TotalTransfers = _options.TotalTransfers,
            InitialTotal = initialTotal,
            FinalTotal = finalTotal,
            ElapsedMs = elapsedMs,
            InvariantPreserved = invariantPreserved,
            NegativeAccounts = negativeAccounts,
            DeadlockDetected = deadlockDetected,
            Notes = notes
        };
    }

    private (Account From, Account To, int Amount) PickTransfer(Random rng)
    {
        int fromIndex = rng.Next(_accounts.Length);
        int toIndex;

        do
        {
            toIndex = rng.Next(_accounts.Length);
        } while (toIndex == fromIndex);

        int amount = rng.Next(1, _options.MaxTransferAmount + 1);

        return (_accounts[fromIndex], _accounts[toIndex], amount);
    }

    private void SequentialTransfer(Account from, Account to, int amount)
    {
        if (from.Balance < amount)
        {
            return;
        }

        from.Balance -= amount;
        to.Balance += amount;
    }

    private void UnsafeTransfer(Account from, Account to, int amount)
    {
        int fromSnapshot = from.Balance;
        if (fromSnapshot < amount)
        {
            return;
        }

        Thread.SpinWait(5000);
        from.Balance = fromSnapshot - amount;

        int toSnapshot = to.Balance;
        Thread.SpinWait(5000);
        to.Balance = toSnapshot + amount;
    }

    private void SafeTransfer(Account from, Account to, int amount)
    {
        Account first = from.Id < to.Id ? from : to;
        Account second = from.Id < to.Id ? to : from;

        lock (first.SyncRoot)
        {
            lock (second.SyncRoot)
            {
                if (from.Balance < amount)
                {
                    return;
                }

                from.Balance -= amount;
                to.Balance += amount;
            }
        }
    }

    private long TotalBalance()
    {
        long total = 0;
        foreach (var account in _accounts)
        {
            total += account.Balance;
        }

        return total;
    }

    private int CountNegativeAccounts()
    {
        int count = 0;
        foreach (var account in _accounts)
        {
            if (account.Balance < 0)
            {
                count++;
            }
        }

        return count;
    }
}

public static class DeadlockDemo
{
    public static SimulationResult Run(SimulationOptions options)
    {
        var accountA = new Account(0, 5000);
        var accountB = new Account(1, 5000);

        long initialTotal = accountA.Balance + accountB.Balance;

        var startSignal = new ManualResetEventSlim(false);
        var stopwatch = Stopwatch.StartNew();

        Thread t1 = new(() =>
        {
            lock (accountA.SyncRoot)
            {
                Console.WriteLine("T1: lock(A)");
                startSignal.Set();
                Thread.Sleep(200);

                lock (accountB.SyncRoot)
                {
                    Console.WriteLine("T1: lock(B)");
                    accountA.Balance -= 100;
                    accountB.Balance += 100;
                }
            }
        });

        Thread t2 = new(() =>
        {
            startSignal.Wait();

            lock (accountB.SyncRoot)
            {
                Console.WriteLine("T2: lock(B)");
                Thread.Sleep(200);

                lock (accountA.SyncRoot)
                {
                    Console.WriteLine("T2: lock(A)");
                    accountB.Balance -= 50;
                    accountA.Balance += 50;
                }
            }
        });

        t1.IsBackground = true;
        t2.IsBackground = true;

        t1.Start();
        t2.Start();

        int timeoutMs = options.DeadlockTimeoutMs;
        var joinBudget = Stopwatch.StartNew();

        bool t1Finished = t1.Join(timeoutMs);
        int remainingMs = Math.Max(0, timeoutMs - (int)joinBudget.ElapsedMilliseconds);
        bool t2Finished = t2.Join(remainingMs);

        stopwatch.Stop();

        bool deadlockDetected = !(t1Finished && t2Finished);
        long finalTotal = accountA.Balance + accountB.Balance;

        return new SimulationResult
        {
            Mode = TransferMode.DeadlockDemo.ToString(),
            AccountsCount = 2,
            ThreadsCount = 2,
            TotalTransfers = 2,
            InitialTotal = initialTotal,
            FinalTotal = finalTotal,
            ElapsedMs = stopwatch.ElapsedMilliseconds,
            InvariantPreserved = !deadlockDetected && initialTotal == finalTotal,
            NegativeAccounts = (accountA.Balance < 0 ? 1 : 0) + (accountB.Balance < 0 ? 1 : 0),
            DeadlockDetected = deadlockDetected,
            Notes = deadlockDetected
                ? "Навмисно відтворено deadlock: потоки захоплюють ресурси у протилежному порядку."
                : "Deadlock не відтворився в цьому запуску; можна повторити запуск."
        };
    }
}
