use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};

pub(crate) struct CopyProgressReporter {
    enabled: bool,
    draw_target: CopyProgressDrawTarget,
    current: Option<CopyProgressFile>,
}

#[derive(Clone, Copy)]
enum CopyProgressDrawTarget {
    Stderr,
    #[cfg(test)]
    Hidden,
}

struct CopyProgressFile {
    name: String,
    size: u64,
    bytes: u64,
    bar: ProgressBar,
}

impl CopyProgressReporter {
    pub(crate) fn new(enabled: bool) -> Self {
        Self {
            enabled,
            draw_target: CopyProgressDrawTarget::Stderr,
            current: None,
        }
    }

    #[cfg(test)]
    fn new_for_test(enabled: bool) -> Self {
        Self {
            enabled,
            draw_target: CopyProgressDrawTarget::Hidden,
            current: None,
        }
    }

    #[cfg(test)]
    fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub(crate) fn begin_file(&mut self, display_name: impl Into<String>, size: u64) {
        if !self.enabled {
            return;
        }
        self.finish_current();
        let name = non_empty_name(&display_name.into(), "copy");
        let draw_target = match self.draw_target {
            CopyProgressDrawTarget::Stderr => ProgressDrawTarget::stderr(),
            #[cfg(test)]
            CopyProgressDrawTarget::Hidden => ProgressDrawTarget::hidden(),
        };
        let bar = ProgressBar::with_draw_target(Some(size), draw_target);
        let style = ProgressStyle::with_template(
            "{msg} {percent:>3}% {bytes}/{total_bytes} {bytes_per_sec} {elapsed_precise}",
        )
        .unwrap_or_else(|_| ProgressStyle::default_bar())
        .progress_chars("=> ");
        bar.set_style(style);
        bar.set_message(name.clone());
        self.current = Some(CopyProgressFile {
            name,
            size,
            bytes: 0,
            bar,
        });
    }

    pub(crate) fn add_bytes(&mut self, bytes: usize) {
        if !self.enabled {
            return;
        }
        if let Some(current) = self.current.as_mut() {
            let bytes = bytes as u64;
            current.bytes = current.bytes.saturating_add(bytes).min(current.size);
            current.bar.inc(bytes);
        }
    }

    pub(crate) fn finish_file(&mut self) {
        if !self.enabled {
            return;
        }
        self.finish_current();
    }

    #[cfg(test)]
    fn current_bytes(&self) -> Option<u64> {
        self.current.as_ref().map(|current| current.bytes)
    }

    #[cfg(test)]
    fn current_name(&self) -> Option<&str> {
        self.current.as_ref().map(|current| current.name.as_str())
    }

    fn finish_current(&mut self) {
        if let Some(current) = self.current.take() {
            current.bar.set_position(current.size);
            current.bar.finish_with_message(format!(
                "{} 100% {}/{}",
                current.name,
                human_bytes(current.size),
                human_bytes(current.size)
            ));
        }
    }
}

impl Drop for CopyProgressReporter {
    fn drop(&mut self) {
        self.finish_current();
    }
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{}{}", bytes, UNITS[unit])
    } else {
        format!("{value:.1}{}", UNITS[unit])
    }
}

fn non_empty_name(value: &str, fallback: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        fallback.to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_progress_reporter_disabled_is_noop() {
        let mut reporter = CopyProgressReporter::new_for_test(false);
        assert!(!reporter.is_enabled());
        reporter.begin_file("file.txt", 10);
        reporter.add_bytes(5);
        assert_eq!(reporter.current_bytes(), None);
        assert_eq!(reporter.current_name(), None);
        reporter.finish_file();
        assert_eq!(reporter.current_bytes(), None);
    }

    #[test]
    fn copy_progress_reporter_tracks_bytes_when_enabled() {
        let mut reporter = CopyProgressReporter::new_for_test(true);
        assert!(reporter.is_enabled());
        reporter.begin_file("file.txt", 10);
        assert_eq!(reporter.current_name(), Some("file.txt"));
        reporter.add_bytes(4);
        reporter.add_bytes(20);
        assert_eq!(reporter.current_bytes(), Some(10));
        reporter.finish_file();
        assert_eq!(reporter.current_bytes(), None);
    }

    #[test]
    fn copy_progress_reporter_handles_zero_byte_file() {
        let mut reporter = CopyProgressReporter::new_for_test(true);
        reporter.begin_file("empty", 0);
        assert_eq!(reporter.current_bytes(), Some(0));
        reporter.finish_file();
        assert_eq!(reporter.current_bytes(), None);
    }
}
