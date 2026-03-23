use std::collections::HashMap;
use std::io;
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Cell, Padding, Paragraph, Row, Table, Tabs};
use ratatui::Terminal;

use ironpipe::{Dag, DagRun, DagRunState, Task, TaskId, TaskState, TriggerRule};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Build DAGs — this is application code, not library presets
    let mut etl = Dag::new("etl_pipeline");
    for id in ["extract", "validate", "transform", "load", "notify"] {
        let mut b = Task::builder(id);
        if id == "transform" { b = b.retries(2); }
        if id == "notify" { b = b.trigger_rule(TriggerRule::AllDone); }
        etl.add_task(b.build()).unwrap();
    }
    etl.chain(&["extract", "validate", "transform", "load", "notify"].map(TaskId::new)).unwrap();

    let mut diamond = Dag::new("diamond_pipeline");
    diamond.add_task(Task::builder("start").build()).unwrap();
    diamond.add_task(Task::builder("branch_a").retries(1).build()).unwrap();
    diamond.add_task(Task::builder("branch_b").build()).unwrap();
    diamond.add_task(Task::builder("join").build()).unwrap();
    diamond.add_task(Task::builder("cleanup").trigger_rule(TriggerRule::Always).build()).unwrap();
    diamond.set_downstream(&TaskId::new("start"), &TaskId::new("branch_a")).unwrap();
    diamond.set_downstream(&TaskId::new("start"), &TaskId::new("branch_b")).unwrap();
    diamond.set_downstream(&TaskId::new("branch_a"), &TaskId::new("join")).unwrap();
    diamond.set_downstream(&TaskId::new("branch_b"), &TaskId::new("join")).unwrap();
    diamond.set_downstream(&TaskId::new("join"), &TaskId::new("cleanup")).unwrap();

    // Print diagram of the diamond DAG to show the library function
    print!("{}", diamond.diagram());

    run_tui(vec![etl, diamond])?;
    Ok(())
}

// =============================================================================
// TUI
// =============================================================================

struct App {
    dags: Vec<Dag>,
    dag_runs: Vec<Option<DagRun>>,
    selected_dag: usize,
    selected_tab: usize,
    should_quit: bool,
    run_states: Vec<HashMap<TaskId, TaskState>>,
    tick_count: usize,
    auto_tick: bool,
}

impl App {
    fn new(dags: Vec<Dag>) -> Self {
        let len = dags.len();
        Self {
            dags,
            dag_runs: (0..len).map(|_| None).collect(),
            selected_dag: 0,
            selected_tab: 0,
            should_quit: false,
            run_states: vec![HashMap::new(); len],
            tick_count: 0,
            auto_tick: false,
        }
    }

    fn current_dag(&self) -> &Dag {
        &self.dags[self.selected_dag]
    }

    fn start_run(&mut self) {
        let dag = self.dags[self.selected_dag].clone();
        let run = DagRun::new(dag, format!("run_{}", self.tick_count), chrono::Utc::now());
        self.dag_runs[self.selected_dag] = Some(run);
        self.run_states[self.selected_dag] = HashMap::new();
        self.tick_count = 0;
        self.auto_tick = false;
    }

    fn tick_run(&mut self) {
        if let Some(ref mut run) = self.dag_runs[self.selected_dag] {
            if run.is_complete() { return; }

            let ready = run.tick();
            for task_id in &ready {
                let _ = run.mark_running(task_id);
            }

            let running: Vec<TaskId> = self.dags[self.selected_dag]
                .task_ids()
                .filter(|id| run.task_state(id) == TaskState::Running && !ready.contains(id))
                .cloned()
                .collect();

            for task_id in &running {
                let _ = run.mark_success(task_id);
            }

            let states: HashMap<TaskId, TaskState> = self.dags[self.selected_dag]
                .task_ids()
                .map(|id| (id.clone(), run.task_state(id)))
                .collect();
            self.run_states[self.selected_dag] = states;
            self.tick_count += 1;
        }
    }
}

fn run_tui(dags: Vec<Dag>) -> Result<(), Box<dyn std::error::Error>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    crossterm::execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new(dags);

    loop {
        terminal.draw(|f| ui(f, &app))?;

        if event::poll(Duration::from_millis(if app.auto_tick { 300 } else { 50 }))?
            && let Event::Key(key) = event::read()?
        {
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => app.should_quit = true,
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => app.should_quit = true,
                KeyCode::Tab => app.selected_tab = (app.selected_tab + 1) % 3,
                KeyCode::BackTab => app.selected_tab = if app.selected_tab == 0 { 2 } else { app.selected_tab - 1 },
                KeyCode::Left => app.selected_dag = if app.selected_dag == 0 { app.dags.len() - 1 } else { app.selected_dag - 1 },
                KeyCode::Right => app.selected_dag = (app.selected_dag + 1) % app.dags.len(),
                KeyCode::Char('r') => app.start_run(),
                KeyCode::Char('t') => app.tick_run(),
                KeyCode::Char('a') => app.auto_tick = !app.auto_tick,
                _ => {}
            }
        }

        if app.auto_tick { app.tick_run(); }
        if app.should_quit { break; }
    }

    disable_raw_mode()?;
    crossterm::execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

// =============================================================================
// TUI rendering (unchanged from before)
// =============================================================================

fn ui(f: &mut ratatui::Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(10),
            Constraint::Length(3),
        ])
        .split(f.area());

    render_title(f, chunks[0]);
    render_dag_selector(f, app, chunks[1]);
    render_tabs(f, app, chunks[2]);

    match app.selected_tab {
        0 => render_dag_structure(f, app, chunks[3]),
        1 => render_task_table(f, app, chunks[3]),
        2 => render_execution_view(f, app, chunks[3]),
        _ => {}
    }

    render_status_bar(f, chunks[4]);
}

fn render_title(f: &mut ratatui::Frame, area: Rect) {
    let title = Paragraph::new(Line::from(vec![
        Span::styled(" ironpipe ", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::styled("DAG task orchestration engine for Rust + Rebar", Style::default().fg(Color::DarkGray)),
    ]))
    .block(Block::default().borders(Borders::ALL).border_type(BorderType::Rounded).border_style(Style::default().fg(Color::Cyan)));
    f.render_widget(title, area);
}

fn render_dag_selector(f: &mut ratatui::Frame, app: &App, area: Rect) {
    let spans: Vec<Span> = app.dags.iter().enumerate().map(|(i, dag)| {
        if i == app.selected_dag {
            Span::styled(format!(" {} ", dag.dag_id), Style::default().fg(Color::Black).bg(Color::Yellow).add_modifier(Modifier::BOLD))
        } else {
            Span::styled(format!(" {} ", dag.dag_id), Style::default().fg(Color::White))
        }
    }).collect();
    let selector = Paragraph::new(Line::from(spans))
        .block(Block::default().title(" DAGs ").borders(Borders::ALL).border_type(BorderType::Rounded).border_style(Style::default().fg(Color::Yellow)));
    f.render_widget(selector, area);
}

fn render_tabs(f: &mut ratatui::Frame, app: &App, area: Rect) {
    let tabs = Tabs::new(vec!["Structure", "Tasks", "Execution"])
        .select(app.selected_tab)
        .style(Style::default().fg(Color::White))
        .highlight_style(Style::default().fg(Color::Black).bg(Color::Green).add_modifier(Modifier::BOLD))
        .divider(" | ")
        .block(Block::default().title(" View (Tab) ").borders(Borders::ALL).border_type(BorderType::Rounded).border_style(Style::default().fg(Color::Green)));
    f.render_widget(tabs, area);
}

fn render_dag_structure(f: &mut ratatui::Frame, app: &App, area: Rect) {
    let dag = app.current_dag();
    let sorted = dag.topological_sort().unwrap_or_default();
    let mut lines: Vec<Line> = Vec::new();

    lines.push(Line::from(vec![
        Span::styled("DAG: ", Style::default().fg(Color::DarkGray)),
        Span::styled(&dag.dag_id, Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::styled(format!("  ({} tasks)", dag.task_count()), Style::default().fg(Color::DarkGray)),
    ]));
    lines.push(Line::from(""));

    for task_id in &sorted {
        let task = dag.get_task(task_id).unwrap();
        let downstream = dag.downstream_of(task_id);
        let state = app.run_states[app.selected_dag].get(task_id).copied().unwrap_or(TaskState::None);
        let (color, icon) = state_style(state);

        let mut spans = vec![
            Span::styled(format!("  {icon} "), Style::default().fg(color)),
            Span::styled(format!("{:<20}", task_id.0), Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
        ];
        if task.trigger_rule != TriggerRule::AllSuccess {
            spans.push(Span::styled(format!(" [{:?}]", task.trigger_rule), Style::default().fg(Color::Magenta)));
        }
        if task.retries > 0 {
            spans.push(Span::styled(format!(" retries={}", task.retries), Style::default().fg(Color::Yellow)));
        }
        lines.push(Line::from(spans));

        if !downstream.is_empty() {
            let mut ds: Vec<&str> = downstream.iter().map(|id| id.0.as_str()).collect();
            ds.sort_unstable();
            lines.push(Line::from(vec![
                Span::styled("       └─▶ ", Style::default().fg(Color::DarkGray)),
                Span::styled(ds.join(", "), Style::default().fg(Color::Blue)),
            ]));
        }
    }

    let block = Block::default().title(" DAG Structure ").borders(Borders::ALL).border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Cyan)).padding(Padding::new(1, 1, 1, 1));
    f.render_widget(Paragraph::new(lines).block(block), area);
}

fn render_task_table(f: &mut ratatui::Frame, app: &App, area: Rect) {
    let dag = app.current_dag();
    let sorted = dag.topological_sort().unwrap_or_default();
    let header = Row::new(["Task ID", "State", "Trigger Rule", "Retries", "Upstream", "Downstream"]
        .map(|s| Cell::from(s).style(Style::default().add_modifier(Modifier::BOLD))))
        .style(Style::default().fg(Color::Yellow));

    let rows: Vec<Row> = sorted.iter().map(|tid| {
        let task = dag.get_task(tid).unwrap();
        let state = app.run_states[app.selected_dag].get(tid).copied().unwrap_or(TaskState::None);
        let (color, icon) = state_style(state);
        let up_set = dag.upstream_of(tid);
        let mut up: Vec<&str> = up_set.iter().map(|id| id.0.as_str()).collect();
        up.sort_unstable();
        let dn_set = dag.downstream_of(tid);
        let mut dn: Vec<&str> = dn_set.iter().map(|id| id.0.as_str()).collect();
        dn.sort_unstable();
        Row::new(vec![
            Cell::from(tid.0.clone()).style(Style::default().add_modifier(Modifier::BOLD)),
            Cell::from(format!("{icon} {state}")).style(Style::default().fg(color)),
            Cell::from(format!("{:?}", task.trigger_rule)),
            Cell::from(format!("{}", task.retries)),
            Cell::from(up.join(", ")),
            Cell::from(dn.join(", ")),
        ])
    }).collect();

    let table = Table::new(rows, [
        Constraint::Length(20), Constraint::Length(18), Constraint::Length(28),
        Constraint::Length(8), Constraint::Min(15), Constraint::Min(15),
    ]).header(header)
    .block(Block::default().title(" Task Details ").borders(Borders::ALL).border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Cyan)).padding(Padding::new(1, 1, 0, 0)));
    f.render_widget(table, area);
}

fn render_execution_view(f: &mut ratatui::Frame, app: &App, area: Rect) {
    let chunks = Layout::default().direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)]).split(area);

    // Left: DAG graph
    let dag = app.current_dag();
    let sorted = dag.topological_sort().unwrap_or_default();
    let mut depth: HashMap<&TaskId, usize> = HashMap::new();
    let mut layers: Vec<Vec<&TaskId>> = Vec::new();
    for tid in &sorted {
        let up = dag.upstream_of(tid);
        let d = if up.is_empty() { 0 } else { up.iter().map(|u| depth.get(u).copied().unwrap_or(0) + 1).max().unwrap_or(0) };
        depth.insert(tid, d);
        while layers.len() <= d { layers.push(Vec::new()); }
        layers[d].push(tid);
    }

    let mut lines: Vec<Line> = vec![
        Line::from(Span::styled("DAG Execution Graph", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))),
        Line::from(""),
    ];
    for (li, layer) in layers.iter().enumerate() {
        if li > 0 {
            lines.push(Line::from(Span::styled(layer.iter().map(|_| "    │     ").collect::<String>(), Style::default().fg(Color::DarkGray))));
            lines.push(Line::from(Span::styled(layer.iter().map(|_| "    ▼     ").collect::<String>(), Style::default().fg(Color::DarkGray))));
        }
        let mut spans = Vec::new();
        for (i, tid) in layer.iter().enumerate() {
            if i > 0 { spans.push(Span::raw("  ")); }
            let state = app.run_states[app.selected_dag].get(*tid).copied().unwrap_or(TaskState::None);
            let (color, icon) = state_style(state);
            spans.push(Span::styled(format!(" {icon} {} ", tid.0), Style::default().fg(color).add_modifier(Modifier::BOLD)));
        }
        lines.push(Line::from(spans));
    }
    f.render_widget(Paragraph::new(lines).block(Block::default().title(" DAG Graph ").borders(Borders::ALL)
        .border_type(BorderType::Rounded).border_style(Style::default().fg(Color::Cyan)).padding(Padding::new(2, 2, 1, 1))), chunks[0]);

    // Right: run info
    let mut lines: Vec<Line> = Vec::new();
    if let Some(ref run) = app.dag_runs[app.selected_dag] {
        lines.push(Line::from(vec![Span::styled("Run ID: ", Style::default().fg(Color::DarkGray)), Span::styled(&run.run_id, Style::default().fg(Color::White))]));
        let rs = run.run_state();
        lines.push(Line::from(vec![Span::styled("State:  ", Style::default().fg(Color::DarkGray)),
            Span::styled(format!("{rs:?}"), Style::default().fg(match rs { DagRunState::Queued => Color::Gray, DagRunState::Running => Color::Blue, DagRunState::Success => Color::Green, DagRunState::Failed => Color::Red }))]));
        lines.push(Line::from(vec![Span::styled("Tick:   ", Style::default().fg(Color::DarkGray)), Span::styled(format!("{}", app.tick_count), Style::default().fg(Color::Yellow))]));
        lines.push(Line::from(vec![Span::styled("Auto:   ", Style::default().fg(Color::DarkGray)),
            Span::styled(if app.auto_tick { "ON" } else { "OFF" }, Style::default().fg(if app.auto_tick { Color::Green } else { Color::Red }))]));
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled("Task States:", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))));
        lines.push(Line::from(""));
        for tid in &sorted {
            let state = run.task_state(tid);
            let attempt = run.attempt_count(tid);
            let (color, icon) = state_style(state);
            let mut spans = vec![
                Span::styled(format!("  {icon} "), Style::default().fg(color)),
                Span::styled(format!("{:<18}", tid.0), Style::default().fg(Color::White)),
                Span::styled(format!("{state:<16}"), Style::default().fg(color)),
            ];
            if attempt > 0 { spans.push(Span::styled(format!("(attempt {attempt})"), Style::default().fg(Color::DarkGray))); }
            lines.push(Line::from(spans));
        }
    } else {
        lines.push(Line::from(Span::styled("No active run", Style::default().fg(Color::DarkGray))));
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled("Press 'r' to start a run", Style::default().fg(Color::Yellow))));
    }
    f.render_widget(Paragraph::new(lines).block(Block::default().title(" Run Status ").borders(Borders::ALL)
        .border_type(BorderType::Rounded).border_style(Style::default().fg(Color::Cyan)).padding(Padding::new(1, 1, 1, 1))), chunks[1]);
}

fn render_status_bar(f: &mut ratatui::Frame, area: Rect) {
    let help = Line::from(vec![
        Span::styled(" q", Style::default().fg(Color::Yellow)), Span::styled(" quit  ", Style::default().fg(Color::DarkGray)),
        Span::styled("←/→", Style::default().fg(Color::Yellow)), Span::styled(" DAG  ", Style::default().fg(Color::DarkGray)),
        Span::styled("Tab", Style::default().fg(Color::Yellow)), Span::styled(" view  ", Style::default().fg(Color::DarkGray)),
        Span::styled("r", Style::default().fg(Color::Yellow)), Span::styled(" run  ", Style::default().fg(Color::DarkGray)),
        Span::styled("t", Style::default().fg(Color::Yellow)), Span::styled(" tick  ", Style::default().fg(Color::DarkGray)),
        Span::styled("a", Style::default().fg(Color::Yellow)), Span::styled(" auto", Style::default().fg(Color::DarkGray)),
    ]);
    f.render_widget(Paragraph::new(help).block(Block::default().borders(Borders::ALL).border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::DarkGray))), area);
}

const fn state_style(state: TaskState) -> (Color, &'static str) {
    match state {
        TaskState::None => (Color::DarkGray, "○"),
        TaskState::Scheduled => (Color::Gray, "◔"),
        TaskState::Queued => (Color::Gray, "◑"),
        TaskState::Running => (Color::Blue, "◉"),
        TaskState::Success => (Color::Green, "●"),
        TaskState::Failed => (Color::Red, "✗"),
        TaskState::Skipped => (Color::Magenta, "⊘"),
        TaskState::UpstreamFailed => (Color::LightRed, "⊗"),
        TaskState::UpForRetry => (Color::Yellow, "↻"),
        TaskState::Removed => (Color::DarkGray, "⊖"),
    }
}
