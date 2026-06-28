using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using StockData.Mcp.Data;
using StockData.Mcp.Data.Entities;
using StockData.Mcp.Fetching;
using StockData.SyncCli.Logging;
using StockData.SyncCli.Progress;
using Terminal.Gui.App;
using Terminal.Gui.Drivers;
using Terminal.Gui.Input;
using Terminal.Gui.ViewBase;
using Terminal.Gui.Views;

namespace StockData.SyncCli.Ui;

/// <summary>
/// Terminal.Gui v2 主窗口：交互终端下的实时同步面板。
///
/// 布局（自顶向下）：
///   ┌─ 标题：stockdata sync — drain --watch ────────────────────┐
///   │ ProgressBar（Done/Total + 百分比）  ……  SpinnerView        │
///   │ Done: 187 / 5232 (3.6%)  Pending: 5034  Partial: 11  Failed │
///   │ ┌── current task ─────────────────────────────────────┐    │
///   │ │ Code:   sh.600000  (full)                          │    │
///   │ │ Steps:  [stock_basic ✓] [k_d ✓] ...                │    │
///   │ └─────────────────────────────────────────────────────┘    │
///   │ ┌── status ────────────────────────────────────────────┐   │
///   │ │ Halted:   10001011 (held 320s / cooldown 600s)       │   │
///   │ │ Elapsed:  1h 23m                                      │   │
///   │ │ ETA:      7h 22m                                      │   │
///   │ │ Speed:    12 codes/min                                │   │
///   │ └───────────────────────────────────────────────────────┘  │
///   │ ┌── log (last 50) ─────────────────────────────────────┐  │
///   │ │ [14:23:01] [WRN] Fetching.Halt: 10001011             │   │
///   │ │ [14:23:05] [INF] SyncEngine: drain step done         │   │
///   │ │ ...                                                   │   │
///   │ └───────────────────────────────────────────────────────┘  │
    ///   │ Toast  ← 短暂反馈（add/retry/restart 结果；3s 后自动隐藏）      │
    ///   │ [a] add stock  [r] retry  [f] restart fetch  [q] quit  [?] help │
///   └────────────────────────────────────────────────────────────┘
///
/// 线程模型：
/// - Drain loop 在后台线程跑（Program.cs 用 Task.Run 启动）；
/// - <see cref="IProgressSource.Updated"/> 回调来自 SyncEngine 的 poller 线程；
/// - log 面板的轮询走 <see cref="IApplication.AddTimeout"/>——回调在主 UI 线程上触发，
///   因此可以直接改 TextView.Text（不需 Invoke）；
/// - 唯一需要 Invoke 的是 progress 回调（poller 线程）。
/// - 键盘快捷键（KeyDown handler）走 UI 线程；retry / restart 的 DB/HTTP 调用通过
///   <see cref="Task.Run"/> 走后台，确认对话框走 <see cref="IApplication.Invoke(Action)"/>
///   回到 UI 线程（Dialog 必须在 UI 线程上 modally run）。
///
/// 退出：
/// - Ctrl-C / q / Esc / Enter → <see cref="IApplication.RequestStop"/> → Terminal.Gui 结束主循环
///   → <see cref="Window.IsRunningChanged"/>（false）→ 这里取消后台 token、注销订阅、RemoveTimeout。
/// - drain 自然完成（队列空 / fetch halt / 异常）→ <see cref="StartDrain"/> 的 finally 块
///   仅 toast 提示 + force refresh，**不**主动 RequestStop，dashboard 保持开着供 d 键重触发。
/// - 必须**不在** Dispose 里清订阅——那时 app 已经拆完，Invoke 抛 ObjectDisposed。
///
/// Drain loop 启动时机：v2 没有「Loaded」事件，drain 在 <c>app.Run(window)</c> 之前由
/// Program.RunTuiModeAsync 调 <see cref="StartDrain"/> 首次启动；运行期间用户按 d 键可在
/// <see cref="HandleShortcut"/> 里重新触发。halt monitor 由 Program.RunTuiModeAsync 独立起，
/// 不与 drain 生命周期绑定。
/// </summary>
public sealed class DashboardWindow : Window
{
    private readonly IApplication _app;
    private readonly IProgressSource _progress;
    private readonly CancellationTokenSource _runCts;
    private readonly IServiceProvider _root;
    private readonly SyncRunService _syncRun;
    private readonly IFetchControl _fetchControl;
    private readonly ILogger<DashboardWindow> _logger;
    private readonly int _restartCooldownSeconds;
    private readonly TuiLoggerProvider? _tuiLogger;

    // L1 子视图
    private readonly ProgressBar _l1Bar;
    private readonly SpinnerView _l1Spinner;
    private readonly Label _l1Stats;

    // L2 当前任务面板
    private readonly Label _l2Code;
    private readonly Label _l2Steps;

    // 状态面板
    private readonly Label _statusHalted;
    private readonly Label _statusElapsed;
    private readonly Label _statusEta;
    private readonly Label _statusSpeed;

    // ── 内容-diff 缓存：避免重渲染时无变化也触发 SetNeedsDraw ──
    // Terminal.Gui 的 Label/ProgressBar setter 即便值相等也会标脏，
    // 1Hz poller 下累积 8 个 setter 全跑一遍 → 每秒 8 次额外 redraw。
    // 用 cache 字段比对旧值，相同就跳过 setter。
    private float _lastFraction = -1f;
    private string? _lastL1Stats;
    private string? _lastL2Code;
    private string? _lastL2Steps;
    private string? _lastStatusHalted;
    private string? _lastStatusElapsed;
    private string? _lastStatusEta;
    private string? _lastStatusSpeed;

    // Log 面板（从 TuiLoggerProvider 的 ring buffer 轮询）
#pragma warning disable CS0618   // TextView 标 obsolete（v2.4.12 引入 EditorView 替代品，但 EditorView 来自独立包 tui-cs/Editor，本项目不引入）
    private readonly TextView? _logView;
#pragma warning restore CS0618
    private readonly object? _logTimeoutToken;     // IApplication.AddTimeout 返回的 token（RemoveTimeout 用）
    // 上一轮 log 快照的尾部时间戳 + 条数：相同 → 跳过 TextView 重写。
    // 比 SequenceEqual 省内存（log 条数大时不开新数组），且不需要保留整个 List。
    private DateTimeOffset? _lastLogTailTs;
    private int _lastLogCount = -1;

    // Stocks 面板：所有已纳管票 + 水位覆盖摘要；超出可视行时支持滚动。独立 5s 轮询（水位变化慢，不需 1Hz）。
    private readonly FrameView _stocksFrame;
    private readonly Label _stocksLabel;
    private object? _stocksTimeoutToken;
    // 上一轮渲染的 text：相同 → 跳过 Label.Text setter（与其它面板同款内容-diff）。
    private string? _lastStocksText;
    // 滚动状态：_allStocks 持有最新一次 5s 刷新拿到的全部票（listview 风格手动滑动窗口）。
    private List<StockEntry> _allStocks = new();
    private int _stocksScrollOffset;
    private int _stocksVisibleRows;
    private bool _userScrolled;

    private sealed record StockEntry(string Code, string Status, int WmCount, string WmLast);

    // Toast + 快捷键栏
    private readonly Label _toast;
    private readonly Label _shortcutsBar;
    private object? _toastTimeoutToken;

    // 重入闸：模态对话框打开期间忽略其它 KeyDown（避免连击重复开 Dialog）
    private bool _modalOpen;

    private bool _subscribed;

    // Drain 可重入：StartDrain 注册 action，StartDrainInternal 启动一个后台 Task；
    // 用户按 d 时再次调 StartDrainInternal 即可重启（_drainRunning 防重入）。
    private Func<CancellationToken, Task>? _drainAction;
    private bool _drainRunning;

    /// <summary>
    /// 构造窗口（装配 UI + 订阅 + 生命周期钩）。drain loop 由 <see cref="StartDrain"/>
    /// 在 <c>app.Run</c> 之前由 caller 启动；运行期间可由 d 键重新触发。
    /// </summary>
    /// <param name="tuiLogger">TUI 模式的日志 ring buffer（<c>null</c> 则不渲染 log 面板）。</param>
    public DashboardWindow(
        IApplication app,
        IProgressSource progress,
        CancellationTokenSource runCts,
        IConfiguration config,
        ILogger<DashboardWindow> logger,
        IServiceProvider root,
        SyncRunService syncRun,
        IFetchControl fetchControl,
        TuiLoggerProvider? tuiLogger = null)
    {
        _app = app;
        _progress = progress;
        _runCts = runCts;
        _logger = logger;
        _root = root;
        _syncRun = syncRun;
        _fetchControl = fetchControl;
        _tuiLogger = tuiLogger;

        _restartCooldownSeconds = config.GetValue("StockData:FetchRestartCooldownSeconds", 600);

        Title = "stockdata sync \u2014 drain --watch";
        Width = Dim.Fill();
        Height = Dim.Fill();

        // ── L1：ProgressBar + Spinner + 统计行 ─────────────────────
        // Spinner 锚定最右侧（占 2 列），ProgressBar 占剩余宽度。
        _l1Bar = new ProgressBar
        {
            X = 0,
            Y = 0,
            Width = Dim.Fill() - 3,   // 给 Spinner 留出 2 列 + 1 间距
            Height = 1,
            ProgressBarFormat = ProgressBarFormat.SimplePlusPercentage,
            Fraction = 0f,
        };
        _l1Spinner = new SpinnerView
        {
            X = Pos.AnchorEnd(2),
            Y = 0,
            Width = 1,
            Height = 1,
            Style = new SpinnerStyle.Dots9(),
            AutoSpin = false,   // 关自旋：AutoSpin=true 时内部 timer 约 10-15Hz 推帧，是刷新抖动的主因。改在 Render() 里 1Hz 手动推一帧
        };
        _l1Stats = new Label
        {
            X = 0,
            Y = Pos.Bottom(_l1Bar),
            Width = Dim.Fill(),
            Height = 1,
            Text = FormatStatsLine(SyncProgress.Empty),
        };
        Add(_l1Bar, _l1Spinner, _l1Stats);

        // ── L2：current task 面板 ─────────────────────────────────
        var currentPane = new FrameView
        {
            X = 0,
            Y = Pos.Bottom(_l1Stats) + 1,
            Width = Dim.Fill(),
            Height = 5,
            Title = "current task",
        };
        _l2Code = new Label { X = 1, Y = 0, Width = Dim.Fill(2), Height = 1, Text = "Code:    (idle \u2014 waiting for tasks)" };
        _l2Steps = new Label { X = 1, Y = Pos.Bottom(_l2Code), Width = Dim.Fill(2), Height = 1, Text = "Steps:   -" };
        currentPane.Add(_l2Code, _l2Steps);
        Add(currentPane);

        // ── Status 面板 ───────────────────────────────────────────
        var statusPane = new FrameView
        {
            X = 0,
            Y = Pos.Bottom(currentPane) + 1,
            Width = Dim.Fill(),
            Height = 6,
            Title = "status",
        };
        _statusHalted = new Label { X = 1, Y = 0, Width = Dim.Fill(2), Height = 1, Text = "Halted:   -" };
        _statusElapsed = new Label { X = 1, Y = Pos.Bottom(_statusHalted), Width = Dim.Fill(2), Height = 1, Text = "Elapsed:  0s" };
        _statusEta = new Label { X = 1, Y = Pos.Bottom(_statusElapsed), Width = Dim.Fill(2), Height = 1, Text = "ETA:      -" };
        _statusSpeed = new Label { X = 1, Y = Pos.Bottom(_statusEta), Width = Dim.Fill(2), Height = 1, Text = "Speed:    -" };
        statusPane.Add(_statusHalted, _statusElapsed, _statusEta, _statusSpeed);
        Add(statusPane);

        // ── Stocks 面板：所有已纳管票 + 水位覆盖摘要，支持滚动 ───────────
        // 嵌在 status 面板正下方、log 面板上方。Display-only Label（无焦点），
        // 5s 轮询一次——水位变化慢，独立于 1Hz progress poller 和 2s log poller。
        // Dim.Fill(N) = parent height - N rows from bottom；
        // N=10 = log(8) + toast gap(1) + shortcuts bar(1)。Dim 与 Pos 独立求值，
        // 所以 log / toast / shortcuts 用 Pos.Bottom / Pos.AnchorEnd 链式定位不冲突。
        _stocksFrame = new FrameView
        {
            X = 0,
            Y = Pos.Bottom(statusPane) + 1,
            Width = Dim.Fill(),
            Height = Dim.Fill(10),
            Title = "stocks",
        };
        _stocksLabel = new Label
        {
            X = 0,
            Y = 0,
            Width = Dim.Fill(),
            Height = Dim.Fill(),
            Text = "(loading...)",
        };
        _stocksFrame.Add(_stocksLabel);
        Add(_stocksFrame);

        // ── Log 面板（TUI 模式专属）───────────────────────────────
        // 之所以需要：Microsoft.Extensions.Logging.Console 写 Console.Out → 与 Terminal.Gui
        // 抢 ANSI 流 → 渲染错位。改由 TuiLoggerProvider 写 ring buffer，dashboard 内部展示。
        FrameView? logFrame = null;
        if (_tuiLogger is not null)
        {
            logFrame = new FrameView
            {
                X = 0,
                Y = Pos.Bottom(_stocksFrame) + 1,
                Width = Dim.Fill(),
                Height = 8,   // 固定 8 行（5 → 8）：底栏 + toast 1 行让 stocksFrame 通过 Dim.Fill 占满
                Title = "log",
            };
#pragma warning disable CS0618   // TextView 标 obsolete（v2.4.12 引入 EditorView 替代品，但 EditorView 来自独立包 tui-cs/Editor，本项目不引入）
            _logView = new TextView
            {
                X = 0,
                Y = 0,
                Width = Dim.Fill(),
                Height = Dim.Fill(),
                ReadOnly = true,
            };
#pragma warning restore CS0618
            logFrame.Add(_logView);
            Add(logFrame);
        }

        // ── Toast + 快捷键栏（最底部）────────────────────────────
        // Toast 紧贴在 log 面板下；shortcuts 在最底行。
        var bottomAnchor = logFrame is not null ? (Pos)Pos.Bottom(logFrame) : Pos.Bottom(statusPane);
        _toast = new Label
        {
            X = 0,
            Y = bottomAnchor + 1,
            Width = Dim.Fill(),
            Height = 1,
            Text = "",
            Visible = false,
        };
        Add(_toast);

        _shortcutsBar = new Label
        {
            X = 0,
            Y = Pos.AnchorEnd(1),
            Width = Dim.Fill(),
            Height = 1,
            Text = ShortcutsText,
        };
        Add(_shortcutsBar);

        // 订阅 + 生命周期钩子
        _progress.Updated += OnProgressUpdated;
        _subscribed = true;

        // Enter / 默认 Accept 命令：触发 RequestStop 走优雅退出
        Accepted += (_, _) => _app.RequestStop();

        // 键盘快捷键：a 弹模态输入；r/f 走确认对话框；q/Esc 退出；? 切帮助。
        // KeyDown 在 UI 主线程触发；handler 自身不能 await，但可以 fire-and-forget
        // 启动 Task 并在 Task 内调 _app.Invoke 切回 UI 线程开 Dialog。
        KeyDown += HandleShortcut;

        // 关键：窗口即将被关掉之前做清理（订阅、后台 token、log timeout、toast timeout）
        IsRunningChanged += OnIsRunningChanged;

        // 首次绘制即渲染一次（让用户看到 0/0 初始态，而不是空白屏直到首个 1Hz 快照）
        Render(SyncProgress.Empty);

        // Log 面板轮询：2000ms 取一次 ring buffer 快照。
        // 原来 500ms 太密，TextView.Text setter 重写整段 → wrap 重算 + 光标复位 + 全量 redraw。
        // Warning+ 级别的日志在长时间 drain 里也少有 < 2s 一条；2s 周期肉眼无感但 redraw 减 4×。
        // AddTimeout 回调在主 UI 线程上执行，可直接改 TextView.Text（不需 Invoke）。
        // 返回 true = 继续调度；返回 false = 停止。token 存起来给 OnIsRunningChanged RemoveTimeout。
        if (_tuiLogger is not null)
        {
            _logTimeoutToken = _app.AddTimeout(TimeSpan.FromMilliseconds(2000), () =>
            {
                try
                {
                    RefreshLogPane();
                    return true;
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "RefreshLogPane 异常");
                    return true;   // 继续调度，不让 timer 死掉（吃掉一次异常比丢调度更好）
                }
            });
        }

        // Stocks 面板：5s 刷一次（水位变化慢，不需 1Hz；独立于 progress poller 和 log pane）。
        // AddTimeout 回调在主 UI 线程上执行——RefreshStocksPaneAsync 内部仍走 _app.Invoke 作为
        // 保险：await 之后的 continuation 可能落到 thread pool，避免 Label.Text setter 跨线程。
        _stocksTimeoutToken = _app.AddTimeout(TimeSpan.FromSeconds(5), () =>
        {
            try
            {
                _ = RefreshStocksPaneAsync();
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "RefreshStocksPane timeout 异常");
                return true;
            }
        });

        // 首次加载：不等 5s timeout，立刻查一次（与 Render(SyncProgress.Empty) 同思路）
        _ = RefreshStocksPaneAsync();
    }

    private const string ShortcutsText = "[a] add stock   [d] drain   [r] retry   [f] restart fetch   [q] quit   [?] help";

    /// <summary>
    /// 从 TuiLoggerProvider 拉快照，与上次比对后写入 TextView。无新条目时跳过 SetText（避免抖动）。
    /// 优化点：原版用 SequenceEqual 比较整个 List → 每次分配新数组 + 全量比较；
    /// 改为比 (count, tailTs)：同 count 同尾时间戳 = 完全没新条目，O(1) 判定；
    /// count 增加但尾时间戳相同 = 不可能（新增必带新 ts），即视为有新条目。
    /// </summary>
    private void RefreshLogPane()
    {
        if (_tuiLogger is null || _logView is null) return;
        var snap = _tuiLogger.Snapshot(LogPaneCapacity);

        // 无新条目 → 跳过 TextView 重写。TextView.Text setter 很重（wrap 重算 + 光标复位 + 全量 redraw）
        var tailTs = snap.Count > 0 ? snap[^1].Timestamp : (DateTimeOffset?)null;
        if (snap.Count == _lastLogCount && tailTs == _lastLogTailTs) return;
        _lastLogCount = snap.Count;
        _lastLogTailTs = tailTs;

        _logView.Text = string.Join('\n', snap.Select(e => e.Format()));
        // Text setter 内部已 SetNeedsDraw；不显式再调
    }

    /// <summary>log 面板里显示的最多条数（最近 N 条）。</summary>
    private const int LogPaneCapacity = 50;

    /// <summary>
    /// 查所有 full stock_sync_task + 每票的 data_watermark 覆盖摘要，存进 _allStocks 后调 RenderStocksPane。
    /// 独立 5s 刷新（不依赖 1Hz progress poller）：水位变化慢，省 CPU。
    /// SQL：单次查询 + 两个相关子查询（count + max），不触发 N+1。
    /// </summary>
    private async Task RefreshStocksPaneAsync()
    {
        try
        {
            await using var scope = _root.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<StockDataDbContext>();

            // 全量取：按 UpdatedAt DESC 排序（最近更新的在前）；无 Take。
            // 子查询在 EF Core 翻译成 SQL 相关子查询（单次 round-trip）。
            var rows = await db.StockSyncTasks.AsNoTracking()
                .Where(t => t.Kind == "full")
                .OrderByDescending(t => t.UpdatedAt)
                .Select(t => new
                {
                    t.Code,
                    t.Status,
                    WmCount = db.DataWatermarks.Count(w => w.Code == t.Code),
                    WmLast = (DateTimeOffset?)db.DataWatermarks
                        .Where(w => w.Code == t.Code)
                        .Max(w => (DateTimeOffset?)w.LastFetchedAt),
                })
                .ToListAsync().ConfigureAwait(false);

            _allStocks = rows.Select(r => new StockEntry(
                r.Code,
                r.Status,
                r.WmCount,
                r.WmLast is null ? "-" : FormatRelative(r.WmLast.Value))).ToList();

            // 用户没动过滚动 → 5s 刷新后回到顶部（最热最新票在前）。
            // 用户动过 → 保留原 offset（但 RenderStocksPane 内部会 clamp 到新边界）。
            if (!_userScrolled) _stocksScrollOffset = 0;

            // RenderStocksPane 自己用 _app.Invoke 切回 UI 线程——timeout 回调已在 UI 线程，
            // 但 await continuation 可能落到 thread pool；Invoke 保险一次。
            _app.Invoke(RenderStocksPane);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "RefreshStocksPane 查询失败");
        }
    }

    /// <summary>
    /// 把 _allStocks 的当前可见窗口（_stocksScrollOffset 起 _stocksVisibleRows 行）格式化到 _stocksLabel。
    /// 必须由 UI 线程调用（已被 RefreshStocksPaneAsync 内部的 _app.Invoke 包好）。
    /// 取可视行数：FrameView 渲染后实际高度 = Frame.Height，内容区 = Frame.Height - 2（border），
    /// Label 第一行是表头，所以数据行 = Frame.Height - 3。
    /// </summary>
    private void RenderStocksPane()
    {
        // Frame 在布局后才有有效高度；首轮 Render 时 frame.Height 可能是 0/1，给个保守下限。
        _stocksVisibleRows = Math.Max(1, _stocksFrame.Frame.Height - 3);

        if (_allStocks.Count == 0)
        {
            const string empty = "stocks (0 registered)\n(no data)";
            SetIfChanged(_stocksLabel, empty, ref _lastStocksText);
            return;
        }

        var total = _allStocks.Count;
        // clamp：滚动后 _allStocks 缩短时不能让 offset 越界
        var maxOffset = Math.Max(0, total - _stocksVisibleRows);
        if (_stocksScrollOffset > maxOffset) _stocksScrollOffset = maxOffset;
        if (_stocksScrollOffset < 0) _stocksScrollOffset = 0;

        var lastShown = Math.Min(_stocksScrollOffset + _stocksVisibleRows, total);
        var title = total <= _stocksVisibleRows
            ? $"stocks ({total} registered)"
            : $"stocks ({total} registered) [{_stocksScrollOffset + 1}-{lastShown}/{total}]";

        var lines = new List<string> { "code         status   wm    last fetch" };
        foreach (var s in _allStocks.Skip(_stocksScrollOffset).Take(_stocksVisibleRows))
            lines.Add($"{s.Code,-12} {s.Status,-8} {s.WmCount,-5} {s.WmLast}");

        SetIfChanged(_stocksLabel, title + "\n" + string.Join('\n', lines), ref _lastStocksText);
    }

    /// <summary>watermark last fetch 相对时间（just now / Nm ago / Nh ago）。</summary>
    private static string FormatRelative(DateTimeOffset t)
    {
        var delta = DateTimeOffset.UtcNow - t;
        return delta.TotalHours >= 1 ? $"{(int)delta.TotalHours}h ago" :
               delta.TotalMinutes >= 1 ? $"{(int)delta.TotalMinutes}m ago" :
               "just now";
    }

    /// <summary>
    /// 注册 drain action 并立即首次触发。drain 完成后 dashboard 保持开着（不 RequestStop），
    /// 改为 toast 提示 + force refresh。用户可按 d 重新触发。
    /// </summary>
    public void StartDrain(Func<CancellationToken, Task> drainAction)
    {
        ArgumentNullException.ThrowIfNull(drainAction);
        _drainAction = drainAction;
        StartDrainInternal();
    }

    private void StartDrainInternal()
    {
        if (_drainRunning || _drainAction is null) return;
        _drainRunning = true;
        var action = _drainAction;
        var token = _runCts.Token;
        _ = Task.Run(async () =>
        {
            try
            {
                await action(token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { /* 正常退出 */ }
            catch (Exception ex)
            {
                _logger.LogError(ex, "drain 异常");
                try { _app.Invoke(() => ShowToast($"drain 异常：{ex.Message}")); }
                catch (ObjectDisposedException) { /* app 已关 */ }
            }
            finally
            {
                _drainRunning = false;
                try
                {
                    _app.Invoke(() =>
                    {
                        ShowToast("drain 完成（按 d 重新触发，按 q 退出）");
                        _ = ForceProgressRefreshAsync();
                    });
                }
                catch (ObjectDisposedException) { /* app 已关 */ }
            }
        }, token);
    }

    private void OnProgressUpdated(SyncProgress snapshot)
    {
        // Poller 线程上 — 切回 UI 主线程再改控件属性
        try
        {
            _app.Invoke(() => Render(snapshot));
        }
        catch (ObjectDisposedException)
        {
            // App 已关；忽略
        }
    }

    private void Render(SyncProgress p)
    {
        // L1 — Fraction round 到 4 位，避免 float 噪声（如 0.001 vs 0.00100001）导致 cache miss
        var fraction = p.Total > 0 ? Math.Clamp((float)p.Done / p.Total, 0f, 1f) : 0f;
        var fractionRounded = (float)Math.Round(fraction, 4);
        if (fractionRounded != _lastFraction)
        {
            _lastFraction = fractionRounded;
            _l1Bar.Fraction = fractionRounded;
        }
        SetIfChanged(_l1Stats, FormatStatsLine(p), ref _lastL1Stats);

        // L2
        if (p.CurrentCode is null)
        {
            SetIfChanged(_l2Code, "Code:    (idle \u2014 waiting for tasks)", ref _lastL2Code);
            SetIfChanged(_l2Steps, "Steps:   -", ref _lastL2Steps);
        }
        else
        {
            SetIfChanged(_l2Code, $"Code:    {p.CurrentCode}  ({p.CurrentKind ?? "\u2014"})", ref _lastL2Code);
            SetIfChanged(_l2Steps, "Steps:   " + StepStatusView.RenderForKind(p.CurrentStepsDone, p.CurrentKind), ref _lastL2Steps);
        }

        // Status
        SetIfChanged(_statusHalted, "Halted:   " + FormatHalted(p.Halted), ref _lastStatusHalted);
        SetIfChanged(_statusElapsed, "Elapsed:  " + FormatSpan(p.Elapsed), ref _lastStatusElapsed);
        SetIfChanged(_statusEta, "ETA:      " + (p.EstimatedRemaining is null ? "-" : FormatSpan(p.EstimatedRemaining.Value)), ref _lastStatusEta);
        SetIfChanged(_statusSpeed, "Speed:    " + FormatSpeed(p), ref _lastStatusSpeed);

        // Spinner：1Hz 手动推一帧（替代 AutoSpin=true 的 10-15Hz 自旋）。
        // 默认 setNeedsDraw=true，让 spinner 字符变化时能正常重绘。
        _l1Spinner.AdvanceAnimation();
    }

    // 缓存比对 + 写入 Label。Terminal.Gui 的 Label.Text setter 会无条件调 SetNeedsDraw，
    // 因此 1Hz 渲染时若不 diff，8 个 Label 全部标脏，叠加 spinner/progressbar 的重绘 → 抖动。
    private static void SetIfChanged(Label label, string text, ref string? cache)
    {
        if (text == cache) return;
        cache = text;
        label.Text = text;
    }

    // ── 文案格式化（pure function，便于 Phase 4 直接断言） ────────────
    internal static string FormatStatsLine(SyncProgress p)
    {
        if (p.Total <= 0)
            return "Done: 0 / 0 (0.0%)  Pending: 0  Partial: 0  Failed: 0";
        var pct = (p.Done * 100.0 / p.Total).ToString("F1");
        return $"Done: {p.Done} / {p.Total} ({pct}%)  Pending: {p.Pending}  Partial: {p.Partial}  Failed: {p.Failed}";
    }

    internal static string FormatHalted(FetchHaltedInfo? halted)
    {
        if (halted is null) return "-";
        var heldSec = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - halted.Since;
        if (heldSec < 0) heldSec = 0;
        return $"{halted.Reason} (held {FormatSpan(TimeSpan.FromSeconds(heldSec))} / cooldown {FormatSpan(TimeSpan.FromSeconds(600))})";
    }

    internal static string FormatSpan(TimeSpan t) =>
        t.TotalHours >= 1 ? $"{(int)t.TotalHours}h{t.Minutes}m" :
        t.TotalMinutes >= 1 ? $"{t.Minutes}m" :   // 1 分钟以上省略秒——避免 "1m 5s" 每秒变一次导致 cache miss
        t.TotalSeconds >= 1 ? $"{(int)t.TotalSeconds}s" :
        "0s";

    internal static string FormatSpeed(SyncProgress p)
    {
        if (p.Done <= 0 || p.Elapsed <= TimeSpan.Zero) return "-";
        var perMin = p.Done / p.Elapsed.TotalMinutes;
        return perMin >= 10
            ? $"{(int)perMin} codes/min"
            : $"{perMin:F1} codes/min";
    }

    // ════════════════════════════════════════════════════════════════
    //  键盘快捷键 + 确认对话框 + Toast
    // ════════════════════════════════════════════════════════════════

    /// <summary>
    /// 键盘快捷键 handler（UI 主线程）。
    /// 重入闸：模态对话框打开期间忽略后续按键，避免重入。
    /// 匹配按 KeyCode：r/R 同 KeyCode.R（小写 r 的 IsShift=false、大写 R 的 IsShift=true，
    /// 但都对应 KeyCode.R）；? 对应 KeyCode='?'（值 63）。
    /// </summary>
    private void HandleShortcut(object? sender, Key key)
    {
        if (_modalOpen) return;
        if (_runCts.IsCancellationRequested) return;   // 正在退出

        // 解 KeyCode 后做匹配（接受大小写，因为 Key.R 和 Key.R.WithShift.KeyCode 都是 R）
        switch (key.KeyCode)
        {
            case KeyCode.A:
                _ = PromptAddStockAsync();
                break;
            case KeyCode.D:
                if (_drainRunning)
                    ShowToast("drain 进行中...");
                else
                {
                    ShowToast("启动 drain...");
                    StartDrainInternal();
                }
                break;
            case KeyCode.R:
                _ = PromptRetryFailedAsync();
                break;
            case KeyCode.F:
                _ = PromptRestartFetchAsync();
                break;
            case KeyCode.Q:
            case KeyCode.Esc:
                _app.RequestStop();
                break;
            case (KeyCode)'?':   // KeyCode 无命名常量；用 char 隐式转换；US kb 报 IsShift=true
            case (KeyCode)'/':   // ? 在 US kb 是 Shift+/，某些键盘布局直接报 KeyCode='/'=47；保险覆盖
                _ = ToggleHelpOverlayAsync();
                break;
            case KeyCode.CursorUp:
            case KeyCode.K:
                ScrollStocks(-1);
                break;
            case KeyCode.CursorDown:
            case KeyCode.J:
                ScrollStocks(+1);
                break;
            default:
                // 其它键不消费，让 View 走默认逻辑
                break;
        }
    }

    /// <summary>
    /// Stocks 面板滚动 +1/-1 行。clamp 到 [0, total - visibleRows]。到达边界就不再动。
    /// UI 线程上跑（KeyDown 触发）；直接 RenderStocksPane 即可，不需 Invoke。
    /// 任何方向位移都置 _userScrolled=true，让下次 5s 刷新不再重置 offset。
    /// </summary>
    private void ScrollStocks(int delta)
    {
        if (_allStocks.Count == 0) return;
        var maxOffset = Math.Max(0, _allStocks.Count - _stocksVisibleRows);
        var next = Math.Clamp(_stocksScrollOffset + delta, 0, maxOffset);
        if (next == _stocksScrollOffset) return;   // 已到边，不重绘
        _stocksScrollOffset = next;
        _userScrolled = true;
        RenderStocksPane();
    }

    /// <summary>
    /// 确认对话框（同步，在 UI 线程上跑）。Yes 返回 true，No/Esc 返回 false。
    /// Terminal.Gui v2 Dialog 用 <see cref="IApplication.Run(IRunnable)"/> 阻塞直到
    /// <see cref="IApplication.RequestStop()"/>；按 <c>IsDefault=true</c> 的按钮在 Enter 时
    /// 会触发 Accepting + RequestStop 链。
    /// </summary>
    private bool ConfirmDialog(string title, string message)
    {
        var result = false;

        var dialog = new Dialog
        {
            Title = title,
            X = Pos.Center(),
            Y = Pos.Center(),
            Width = Math.Min(72, Math.Max(40, message.Length + 6)),
            Height = 7,
        };

        var msg = new Label
        {
            X = 1,
            Y = 0,
            Width = Dim.Fill(2),
            Height = 3,
            Text = message,
        };

        var yes = new Button
        {
            Text = "Yes",
            X = Pos.Center() - 10,
            Y = 4,
            IsDefault = false,
        };
        var no = new Button
        {
            Text = "No",
            X = Pos.Center() + 4,
            Y = 4,
            IsDefault = true,    // Enter 默认取消（更安全）
        };

        yes.Accepting += (_, _) =>
        {
            result = true;
            _app.RequestStop();
        };
        no.Accepting += (_, _) =>
        {
            result = false;
            _app.RequestStop();
        };

        dialog.Add(msg, yes, no);

        _modalOpen = true;
        try
        {
            _app.Run(dialog);
        }
        finally
        {
            _modalOpen = false;
        }
        return result;
    }

    /// <summary>
    /// r：retry failed tasks（带确认）。
    /// 流程：UI 线程（KeyDown）→ 后台 Task 数 failed 计数 → 通过 TaskCompletionSource
    /// 把 Dialog 结果拿回 async 上下文 → Yes 则调 RetryFailedAsync → toast 显示。
    /// </summary>
    private async Task PromptRetryFailedAsync()
    {
        try
        {
            int failedCount = await Task.Run(async () =>
            {
                await using var scope = _root.CreateAsyncScope();
                var db = scope.ServiceProvider.GetRequiredService<StockDataDbContext>();
                return await db.StockSyncTasks.AsNoTracking().CountAsync(t => t.Status == "failed");
            }).ConfigureAwait(false);

            bool confirmed = false;
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _app.Invoke(() =>
            {
                try
                {
                    if (failedCount == 0)
                    {
                        ShowToast("当前没有 failed 任务可重置");
                        tcs.TrySetResult(false);
                        return;
                    }
                    tcs.TrySetResult(ConfirmDialog(
                        title: "retry failed",
                        message: $"重置 {failedCount} 个 failed 任务为 pending？\n" +
                                 $"下次 drain 会捡起重试（Coverage 跳过已新鲜，只补仍不新鲜的数据集）。\n\n" +
                                 $"Enter 取消，Y 确认。"));
                }
                catch (Exception ex)
                {
                    tcs.TrySetException(ex);
                }
            });
            confirmed = await tcs.Task.ConfigureAwait(false);

            if (!confirmed) return;

            var result = await Task.Run(() => _syncRun.RetryFailedAsync()).ConfigureAwait(false);
            var retried = ExtractRetried(result);
            _app.Invoke(() =>
            {
                ShowToast($"已重置 {retried} 个任务为 pending");
                _ = ForceProgressRefreshAsync();
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "retry failed 流程异常");
            try { _app.Invoke(() => ShowToast($"retry 失败：{ex.Message}")); }
            catch (ObjectDisposedException) { /* app 已关 */ }
        }
    }

    /// <summary>
    /// f：restart fetch（带确认）。
    /// 流程：先 GET /status 拿当前 halt 态 → Dialog 显示 → Yes 则 POST /restart → toast。
    /// </summary>
    private async Task PromptRestartFetchAsync()
    {
        try
        {
            FetchStatusResponse? status = null;
            string statusErr = "";
            try
            {
                status = await Task.Run(() => _fetchControl.GetStatusAsync()).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "fetch /status 探测失败");
                statusErr = ex.Message;
            }

            bool confirmed = false;
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _app.Invoke(() =>
            {
                try
                {
                    string haltInfo;
                    if (statusErr.Length > 0)
                        haltInfo = $"fetch 状态未知（{statusErr}）；强制 /restart 仍可尝试";
                    else if (status is { IsHalted: true })
                        haltInfo = $"halted: {status.Halted?.Reason}（持续 {HaltedSeconds(status)}s；冷却 {_restartCooldownSeconds}s）";
                    else
                        haltInfo = "running（无 halt；强制 /restart 会清空 job 队列）";

                    tcs.TrySetResult(ConfirmDialog(
                        title: "restart fetch",
                        message: $"立即 POST fetch /restart？\n当前状态：{haltInfo}\n\nEnter 取消，Y 确认。"));
                }
                catch (Exception ex)
                {
                    tcs.TrySetException(ex);
                }
            });
            confirmed = await tcs.Task.ConfigureAwait(false);

            if (!confirmed) return;

            _app.Invoke(() => ShowToast("restart 中..."));
            var result = await Task.Run(() => _fetchControl.RestartAsync()).ConfigureAwait(false);
            var wasHalted = result?.WasHalted == true;
            _app.Invoke(() => ShowToast($"restart 完成（was_halted={wasHalted}）"));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "fetch /restart 流程异常");
            try { _app.Invoke(() => ShowToast($"restart 失败：{ex.Message}")); }
            catch (ObjectDisposedException) { /* app 已关 */ }
        }
    }

    /// <summary>
    /// a：纳管新股票。弹模态输入框收 code → 校验 → 调 <see cref="SyncRegistry.RegisterIfNewAsync"/>
    /// （写 synced_stock + 插 pending task）。drain 下轮 NextDueAsync 会捡起。
    /// 流程同 <see cref="PromptRetryFailedAsync"/>：UI 线程 KeyDown → TaskCompletionSource
    /// 桥接 modal Dialog → 后台 DB 写入 → UI toast。
    /// </summary>
    private async Task PromptAddStockAsync()
    {
        string? code = null;
        var tcs = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        _app.Invoke(() =>
        {
            try { tcs.TrySetResult(PromptForCodeDialog()); }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "PromptForCodeDialog 异常");
                tcs.TrySetResult(null);
            }
        });
        code = await tcs.Task.ConfigureAwait(false);

        // 空 / 取消 / Esc → 不动
        if (string.IsNullOrWhiteSpace(code)) return;
        code = code.Trim();

        // 格式校验：复用 CodeNormalizer 的正则（避免再次手写 sh./sz./bj. + 6 位数字）
        if (!CodeNormalizer.IsValid(code))
        {
            _app.Invoke(() => ShowToast(
                $"无效代码：{code}（期望 sh.600000 / sz.000001 / bj.430047）"));
            return;
        }

        _app.Invoke(() => ShowToast($"纳管中 {code}..."));
        try
        {
            await Task.Run(async () =>
            {
                await using var scope = _root.CreateAsyncScope();
                var db = scope.ServiceProvider.GetRequiredService<StockDataDbContext>();
                await SyncRegistry.RegisterIfNewAsync(db, code!);
                await db.SaveChangesAsync().ConfigureAwait(false);
            }).ConfigureAwait(false);
            _app.Invoke(() =>
            {
                ShowToast($"已纳管 {code}（pending 任务已建）");
                _ = ForceProgressRefreshAsync();
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "纳管 {Code} 失败", code);
            try { _app.Invoke(() => ShowToast($"纳管失败：{ex.Message}")); }
            catch (ObjectDisposedException) { /* app 已关 */ }
        }
    }

    /// <summary>
    /// 「add stock」模态输入框（UI 主线程，同步执行）。
    /// OK 返回 trimmed 非空字符串；Cancel / Esc / 空输入返回 null。
    /// </summary>
    private string? PromptForCodeDialog()
    {
        string? result = null;

        var dialog = new Dialog
        {
            Title = "add stock",
            X = Pos.Center(),
            Y = Pos.Center(),
            Width = 60,
            Height = 10,
        };

        var prompt = new Label
        {
            X = 1,
            Y = 1,
            Width = Dim.Fill() - 2,
            Height = 1,
            Text = "股票代码（如 sh.600000 / sz.000001）",
        };

        var input = new TextField
        {
            Text = "",
            X = 1,
            Y = 3,
            Width = Dim.Fill() - 2,
            Height = 1,
        };

        // OK = IsDefault（Enter 直接确认）；Esc 由 Dialog 自身绑到 Command.Cancel → RequestStop，
        // 此时 result 仍为 null 初始值（无需 Cancel.Accepting 也行；显式点 Cancel 按钮走 Accepting）
        var ok = new Button { Text = "OK", X = Pos.Center() - 10, Y = 5, IsDefault = true };
        var cancel = new Button { Text = "Cancel", X = Pos.Center() + 3, Y = 5 };

        ok.Accepting += (_, _) =>
        {
            var txt = input.Text;
            result = string.IsNullOrWhiteSpace(txt) ? null : txt.Trim();
            _app.RequestStop();
        };
        cancel.Accepting += (_, _) =>
        {
            result = null;
            _app.RequestStop();
        };

        // 焦点默认落在第一个 button 上；显式切到 TextField 让用户能直接打字
        input.SetFocus();
        dialog.Add(prompt, input, ok, cancel);

        _modalOpen = true;
        try
        {
            _app.Run(dialog);
        }
        finally
        {
            _modalOpen = false;
        }
        return result;
    }

    /// <summary>
    /// ?：toggle 帮助覆盖层。
    /// 实现：开一个只读 Dialog 列出快捷键，按 OK 关闭。
    /// </summary>
    private Task ToggleHelpOverlayAsync()
    {
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        _app.Invoke(() =>
        {
            try
            {
                var help = new Dialog
                {
                    Title = "shortcuts",
                    X = Pos.Center(),
                    Y = Pos.Center(),
                    Width = 60,
                    Height = 13,
                };
                var text = new Label
                {
                    X = 1,
                    Y = 0,
                    Width = Dim.Fill(2),
                    Height = 9,
                    Text =
                        "  [a]  add stock（输入 code 写 synced_stock + pending task）\n" +
                        "  [d]  drain（重新触发同步，处理新加入/重置的任务）\n" +
                        "  [r]  retry failed tasks（reset failed → pending）\n" +
                        "  [f]  restart fetch（POST /restart）\n" +
                        "  [q]  quit dashboard（同 Ctrl-C）\n" +
                        "  [?]  toggle this help\n" +
                        "  [↑/↓]  scroll stocks pane（or k/j）\n\n" +
                        "  Enter 关闭。retry / restart 都需要确认。",
                };
                var ok = new Button { Text = "OK", X = Pos.Center(), Y = 7, IsDefault = true };
                ok.Accepting += (_, _) => _app.RequestStop();
                help.Add(text, ok);

                _modalOpen = true;
                try { _app.Run(help); }
                finally { _modalOpen = false; }
                tcs.TrySetResult(true);
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }
        });
        return tcs.Task;
    }

    /// <summary>
    /// Toast：在底栏上方显示一行文本，3 秒后自动隐藏。
    /// 用 AddTimeout（主 UI 线程）调度隐藏，不阻塞 KeyDown handler。
    /// </summary>
    private void ShowToast(string message)
    {
        try
        {
            _toast.Text = message;
            _toast.Visible = true;
            _toast.SetNeedsDraw();

            // 取消上一个 timer（若用户在 3s 内又触发了新 toast）
            if (_toastTimeoutToken is not null)
            {
                try { _app.RemoveTimeout(_toastTimeoutToken); }
                catch (ObjectDisposedException) { return; }
                _toastTimeoutToken = null;
            }

            _toastTimeoutToken = _app.AddTimeout(TimeSpan.FromSeconds(3), () =>
            {
                try
                {
                    _toast.Visible = false;
                    _toast.Text = "";
                    _toast.SetNeedsDraw();
                }
                catch (ObjectDisposedException) { /* app 已关 */ }
                _toastTimeoutToken = null;
                return false;   // 不再调度
            });
        }
        catch (ObjectDisposedException) { /* app 已关 */ }
    }

    private static long HaltedSeconds(FetchStatusResponse s)
    {
        if (s.Halted is null) return 0;
        return Math.Max(0, DateTimeOffset.UtcNow.ToUnixTimeSeconds() - s.Halted.Since);
    }

    /// <summary>跳过 1Hz 等待，立即查一次进度并 emit 快照。add stock / retry 后调用。</summary>
    private async Task ForceProgressRefreshAsync()
    {
        try
        {
            var engine = _root.GetRequiredService<SyncEngine>();
            await engine.ForcePollAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "ForceProgressRefresh 失败（忽略）");
        }
    }

    private static int ExtractRetried(object? retryResult)
    {
        // SyncRunService.RetryFailedAsync 返回 anonymous { retried = N }——用反射拿 retried
        if (retryResult is null) return 0;
        var prop = retryResult.GetType().GetProperty("retried");
        if (prop?.GetValue(retryResult) is int n) return n;
        if (prop?.GetValue(retryResult) is long nl) return (int)nl;
        return 0;
    }

    private void OnIsRunningChanged(object? sender, EventArgs<bool> e)
    {
        // Window.IsRunning 在 app 拆掉前转 false；这里是唯一安全的清理点
        if (e.Value) return;
        if (_subscribed)
        {
            _progress.Updated -= OnProgressUpdated;
            _subscribed = false;
        }
        if (_logTimeoutToken is not null)
        {
            try { _app.RemoveTimeout(_logTimeoutToken); } catch (ObjectDisposedException) { /* app 已关 */ }
        }
        if (_stocksTimeoutToken is not null)
        {
            try { _app.RemoveTimeout(_stocksTimeoutToken); } catch (ObjectDisposedException) { /* app 已关 */ }
            _stocksTimeoutToken = null;
        }
        if (_toastTimeoutToken is not null)
        {
            try { _app.RemoveTimeout(_toastTimeoutToken); } catch (ObjectDisposedException) { /* app 已关 */ }
        }
        try { if (!_runCts.IsCancellationRequested) _runCts.Cancel(); } catch (ObjectDisposedException) { }
        _logger.LogInformation("dashboard closed");
    }
}