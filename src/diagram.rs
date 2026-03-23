use std::collections::HashMap;

use crate::dag::Dag;
use crate::task_id::TaskId;
use crate::trigger_rule::TriggerRule;

/// Render a DAG as a horizontal (left-to-right) ASCII diagram.
#[must_use]
pub fn horizontal(dag: &Dag) -> String {
    let Ok(sorted) = dag.topological_sort() else {
        return "Error: DAG contains a cycle".to_string();
    };
    let layers = compute_layers(dag, &sorted);
    let mut out = String::new();
    out.push_str(&title(dag));
    out.push_str(&render_horizontal(dag, &layers));
    out.push_str(&summary(dag, &sorted));
    out
}

/// Render a DAG as a vertical (top-to-bottom) ASCII diagram.
#[must_use]
pub fn vertical(dag: &Dag) -> String {
    let Ok(sorted) = dag.topological_sort() else {
        return "Error: DAG contains a cycle".to_string();
    };
    let layers = compute_layers(dag, &sorted);
    let mut out = String::new();
    out.push_str(&title(dag));
    out.push_str(&render_vertical(dag, &layers));
    out.push_str(&summary(dag, &sorted));
    out
}

fn compute_layers<'a>(dag: &Dag, sorted: &'a [TaskId]) -> Vec<Vec<&'a TaskId>> {
    let mut depth: HashMap<&TaskId, usize> = HashMap::new();
    let mut layers: Vec<Vec<&TaskId>> = Vec::new();

    for task_id in sorted {
        let upstream = dag.upstream_of(task_id);
        let d = if upstream.is_empty() {
            0
        } else {
            upstream
                .iter()
                .map(|u| depth.get(u).copied().unwrap_or(0) + 1)
                .max()
                .unwrap_or(0)
        };
        depth.insert(task_id, d);
        while layers.len() <= d {
            layers.push(Vec::new());
        }
        layers[d].push(task_id);
    }
    layers
}

fn title(dag: &Dag) -> String {
    let t = format!("DAG: {}", dag.dag_id);
    let border = "═".repeat(t.len() + 2);
    format!("\n╔{border}╗\n║ {t} ║\n╚{border}╝\n\n")
}

fn summary(dag: &Dag, sorted: &[TaskId]) -> String {
    format!(
        "\n  Tasks: {}\n  Roots: {}\n  Leaves: {}\n  Topo order: {}\n\n",
        dag.task_count(),
        dag.roots().iter().map(|id| id.0.as_str()).collect::<Vec<_>>().join(", "),
        dag.leaves().iter().map(|id| id.0.as_str()).collect::<Vec<_>>().join(", "),
        sorted.iter().map(|id| id.0.as_str()).collect::<Vec<_>>().join(" → "),
    )
}

fn label_attr(dag: &Dag, task_id: &TaskId) -> String {
    let task = dag.get_task(task_id).unwrap();
    let mut attrs = Vec::new();
    if task.trigger_rule != TriggerRule::AllSuccess {
        attrs.push(format!("{:?}", task.trigger_rule));
    }
    if task.retries > 0 {
        attrs.push(format!("retries={}", task.retries));
    }
    attrs.join(" ")
}

fn draw_box(grid: &mut [Vec<char>], row: usize, col: usize, width: usize, name: &str, attr: &str) {
    let inner = width - 2;

    set_str(grid, row, col, &format!("┌{}┐", "─".repeat(inner)));

    let name_trunc: String = name.chars().take(inner).collect();
    let pad = inner.saturating_sub(name_trunc.len());
    let lp = pad / 2;
    let rp = pad - lp;
    set_str(grid, row + 1, col, &format!("│{}{}{}│", " ".repeat(lp), name_trunc, " ".repeat(rp)));

    let attr_trunc: String = attr.chars().take(inner).collect();
    let sp = inner.saturating_sub(attr_trunc.len());
    let al = sp / 2;
    let ar = sp - al;
    set_str(grid, row + 2, col, &format!("│{}{}{}│", " ".repeat(al), attr_trunc, " ".repeat(ar)));

    set_str(grid, row + 3, col, &format!("└{}┘", "─".repeat(inner)));
}

fn set_str(grid: &mut [Vec<char>], row: usize, col: usize, s: &str) {
    for (i, ch) in s.chars().enumerate() {
        if row < grid.len() && col + i < grid[row].len() {
            grid[row][col + i] = ch;
        }
    }
}

// ─── Horizontal (left-to-right) ─────────────────────────────────────────────

#[allow(clippy::too_many_lines, clippy::needless_range_loop)]
fn render_horizontal(dag: &Dag, layers: &[Vec<&TaskId>]) -> String {
    let box_width = 20;
    let h_gap = 5;
    let row_height = 4;
    let v_gap = 1;

    let max_rows = layers.iter().map(Vec::len).max().unwrap_or(1);
    let total_rows = max_rows * (row_height + v_gap);
    let total_cols = layers.len() * (box_width + h_gap);
    let mut grid: Vec<Vec<char>> = vec![vec![' '; total_cols]; total_rows];
    let mut centers: HashMap<&TaskId, (usize, usize)> = HashMap::new();

    for (layer_idx, layer) in layers.iter().enumerate() {
        let col_start = layer_idx * (box_width + h_gap);
        let total_task_height = layer.len() * row_height + (layer.len().saturating_sub(1)) * v_gap;
        let row_offset = (total_rows.saturating_sub(total_task_height)) / 2;

        for (task_idx, task_id) in layer.iter().enumerate() {
            let row_start = row_offset + task_idx * (row_height + v_gap);
            draw_box(&mut grid, row_start, col_start, box_width, &task_id.0, &label_attr(dag, task_id));
            centers.insert(task_id, (col_start, row_start + row_height / 2));
        }
    }

    // Draw connectors
    for (layer_idx, layer) in layers.iter().enumerate() {
        if layer_idx == 0 { continue; }
        for task_id in layer {
            let upstream = dag.upstream_of(task_id);
            let &(dest_col, dest_row) = centers.get(task_id).unwrap();
            for up_id in &upstream {
                if let Some(&(src_col, src_row)) = centers.get(up_id) {
                    let arrow_start = src_col + box_width;
                    let arrow_end = dest_col;
                    if src_row == dest_row {
                        for c in (arrow_start + 1)..arrow_end {
                            if grid[src_row][c] == ' ' { grid[src_row][c] = '─'; }
                        }
                        if arrow_end > 0 { grid[src_row][arrow_end - 1] = '▶'; }
                    } else {
                        let mid_col = arrow_start + (arrow_end - arrow_start) / 2;
                        for c in (arrow_start + 1)..=mid_col {
                            if grid[src_row][c] == ' ' { grid[src_row][c] = '─'; }
                        }
                        let (top, bot) = if src_row < dest_row { (src_row, dest_row) } else { (dest_row, src_row) };
                        for r in top..=bot {
                            if grid[r][mid_col] == '─' { grid[r][mid_col] = '┼'; }
                            else if grid[r][mid_col] == ' ' { grid[r][mid_col] = '│'; }
                        }
                        grid[src_row][mid_col] = if src_row < dest_row { '┐' } else { '┘' };
                        grid[dest_row][mid_col] = if src_row < dest_row { '└' } else { '┌' };
                        for c in (mid_col + 1)..arrow_end {
                            if grid[dest_row][c] == ' ' { grid[dest_row][c] = '─'; }
                        }
                        if arrow_end > 0 { grid[dest_row][arrow_end - 1] = '▶'; }
                    }
                }
            }
        }
    }

    let mut out = String::new();
    for row in &grid {
        let line: String = row.iter().collect();
        let trimmed = line.trim_end();
        if !trimmed.is_empty() {
            out.push_str(trimmed);
            out.push('\n');
        }
    }
    out
}

// ─── Vertical (top-to-bottom) ───────────────────────────────────────────────

#[allow(clippy::too_many_lines)]
fn render_vertical(dag: &Dag, layers: &[Vec<&TaskId>]) -> String {
    let col_width = 22;
    let max_cols = layers.iter().map(Vec::len).max().unwrap_or(1);
    let total_width = max_cols * col_width;
    let mut out = String::new();

    for (layer_idx, layer) in layers.iter().enumerate() {
        if layer_idx > 0 {
            let prev = &layers[layer_idx - 1];
            let mut conn = vec![' '; total_width];
            let mut arrow = vec![' '; total_width];

            for task_id in layer {
                let upstream = dag.upstream_of(task_id);
                let task_col = layer.iter().position(|t| t == task_id).unwrap();
                let tc = vcenter(task_col, layer.len(), total_width);

                for up_id in &upstream {
                    if let Some(up_col) = prev.iter().position(|t| *t == up_id) {
                        let uc = vcenter(up_col, prev.len(), total_width);
                        let (left, right) = if uc <= tc { (uc, tc) } else { (tc, uc) };
                        if uc == tc {
                            if conn[tc] == ' ' { conn[tc] = '│'; }
                        } else {
                            for ch in &mut conn[left..=right] {
                                if *ch == ' ' { *ch = '─'; }
                                else if *ch == '│' { *ch = '┼'; }
                            }
                            conn[uc] = if conn[uc] == '─' { '┴' } else { '│' };
                            conn[tc] = if conn[tc] == '─' { '┬' } else { '│' };
                        }
                    }
                }
                if arrow[tc] == ' ' { arrow[tc] = '▼'; }
            }

            let cs: String = conn.iter().collect();
            let as_: String = arrow.iter().collect();
            out.push_str(cs.trim_end());
            out.push('\n');
            out.push_str(as_.trim_end());
            out.push('\n');
        }

        let box_w = 18;
        let mut l1 = vec![' '; total_width];
        let mut l2 = vec![' '; total_width];
        let mut l3 = vec![' '; total_width];
        let mut l4 = vec![' '; total_width];

        for (ci, task_id) in layer.iter().enumerate() {
            let task = dag.get_task(task_id).unwrap();
            let center = vcenter(ci, layer.len(), total_width);
            let left = center.saturating_sub(box_w / 2);

            let top = format!("┌{}┐", "─".repeat(box_w));
            for (i, ch) in top.chars().enumerate() { if left + i < total_width { l1[left + i] = ch; } }

            let name = &task_id.0;
            let np = box_w.saturating_sub(name.len());
            let mid = format!("│{}{}{}│", " ".repeat(np / 2), name, " ".repeat(np - np / 2));
            for (i, ch) in mid.chars().enumerate() { if left + i < total_width { l2[left + i] = ch; } }

            let mut attrs = Vec::new();
            if task.trigger_rule != TriggerRule::AllSuccess { attrs.push(format!("{:?}", task.trigger_rule)); }
            if task.retries > 0 { attrs.push(format!("retries={}", task.retries)); }
            let a = attrs.join(" ");
            let ap = box_w.saturating_sub(a.len());
            let aline = format!("│{}{}{}│", " ".repeat(ap / 2), a, " ".repeat(ap - ap / 2));
            for (i, ch) in aline.chars().enumerate() { if left + i < total_width { l3[left + i] = ch; } }

            let bot = format!("└{}┘", "─".repeat(box_w));
            for (i, ch) in bot.chars().enumerate() { if left + i < total_width { l4[left + i] = ch; } }
        }

        for line in [&l1, &l2, &l3, &l4] {
            let s: String = line.iter().collect();
            out.push_str(s.trim_end());
            out.push('\n');
        }
    }
    out
}

const fn vcenter(col_idx: usize, num_cols: usize, total_width: usize) -> usize {
    if num_cols == 1 {
        total_width / 2
    } else {
        let spacing = total_width / num_cols;
        spacing / 2 + col_idx * spacing
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::Task;

    #[test]
    fn horizontal_renders_linear_dag() {
        let mut dag = Dag::new("test");
        dag.add_task(Task::builder("a").build()).unwrap();
        dag.add_task(Task::builder("b").build()).unwrap();
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("b")).unwrap();

        let out = horizontal(&dag);
        assert!(out.contains("DAG: test"));
        assert!(out.contains("a"));
        assert!(out.contains("b"));
        assert!(out.contains("───▶"));
    }

    #[test]
    fn vertical_renders_diamond_dag() {
        let mut dag = Dag::new("diamond");
        dag.add_task(Task::builder("a").build()).unwrap();
        dag.add_task(Task::builder("b").build()).unwrap();
        dag.add_task(Task::builder("c").build()).unwrap();
        dag.add_task(Task::builder("d").build()).unwrap();
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("b")).unwrap();
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("c")).unwrap();
        dag.set_downstream(&TaskId::new("b"), &TaskId::new("d")).unwrap();
        dag.set_downstream(&TaskId::new("c"), &TaskId::new("d")).unwrap();

        let out = vertical(&dag);
        assert!(out.contains("DAG: diamond"));
        assert!(out.contains("▼"));
    }

    #[test]
    fn shows_trigger_rule_and_retries() {
        let mut dag = Dag::new("attrs");
        dag.add_task(Task::builder("a").build()).unwrap();
        dag.add_task(Task::builder("b").trigger_rule(TriggerRule::Always).retries(3).build()).unwrap();
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("b")).unwrap();

        let out = horizontal(&dag);
        assert!(out.contains("Always"));
        assert!(out.contains("retries=3"));
    }

    #[test]
    fn cycle_returns_error() {
        let mut dag = Dag::new("cycle");
        dag.add_task(Task::builder("a").build()).unwrap();
        dag.add_task(Task::builder("b").build()).unwrap();
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("b")).unwrap();
        dag.set_downstream(&TaskId::new("b"), &TaskId::new("a")).unwrap();

        let out = horizontal(&dag);
        assert!(out.contains("cycle"));
    }
}
