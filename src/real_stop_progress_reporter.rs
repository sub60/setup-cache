use core::error::Error;
use core::num::NonZeroU64;
use core::{fmt, iter};

use futures::{AsyncWrite, AsyncWriteExt};
use nix_types::StorePath;

use crate::protocol::StoreDir;
use crate::{context, event_loop};

pub(crate) struct RealStopProgressReporter<W> {
    /// Whether there were still store paths to handle when the daemon was told
    /// to stop.
    had_paths_left_to_handle: bool,
    writer: W,
}

/// The report written once every store path has been handled.
struct FinalReport<'a, Ctx: context::Context>(
    &'a event_loop::ActionReport<Ctx>,
);

/// A number of things, shown with their name in the singular or plural.
struct Count<'a>(u32, &'a str);

/// An error followed by its causes.
pub(crate) enum WithCauses<'a> {
    /// Joined with colons on a single line.
    Inline(&'a dyn Error),

    /// Each on its own line under an indented `Caused by:` header, numbered
    /// when there's more than one.
    Indented(&'a dyn Error),
}

impl<W> RealStopProgressReporter<W> {
    pub(crate) fn new(writer: W) -> Self {
        Self { had_paths_left_to_handle: false, writer }
    }
}

impl<W: AsyncWrite + Unpin> context::StopProgressReporter
    for RealStopProgressReporter<W>
{
    async fn report_paths_left_to_handle(
        &mut self,
        num_paths_left_to_handle: u32,
    ) {
        if num_paths_left_to_handle == 0 {
            return;
        }
        // The progress lines go in a group, which GitHub shows folded, and
        // which the final report closes.
        let msg = format!(
            "::group::Handling the remaining {}\n",
            Count(num_paths_left_to_handle, "store path")
        );
        let _ = self.writer.write_all(msg.as_bytes()).await;
        self.had_paths_left_to_handle = true;
    }

    async fn report_path_pushed(
        &mut self,
        path: &StorePath<StoreDir>,
        _: NonZeroU64,
    ) {
        let msg = format!("Pushed {path}\n");
        let _ = self.writer.write_all(msg.as_bytes()).await;
    }

    async fn report_path_skipped(&mut self, path: &StorePath<StoreDir>) {
        let msg = format!("Skipped {path}\n");
        let _ = self.writer.write_all(msg.as_bytes()).await;
    }

    async fn report_path_handling_error<C: context::Cache, N: context::Nix>(
        &mut self,
        path: &StorePath<StoreDir>,
        _error: &event_loop::HandlePathError<C, N>,
    ) {
        let msg = format!("Failed handling {path}\n");
        let _ = self.writer.write_all(msg.as_bytes()).await;
    }

    async fn report_final_report<Ctx: context::Context>(
        mut self,
        report: event_loop::ActionReport<Ctx>,
    ) {
        let mut msg = String::new();

        if self.had_paths_left_to_handle {
            msg.push_str("::endgroup::\n");
        }

        msg.push_str(&FinalReport(&report).to_string());

        let _ = self.writer.write_all(msg.as_bytes()).await;
        let _ = self.writer.flush().await;
    }
}

impl<Ctx: context::Context> fmt::Display for FinalReport<'_, Ctx> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let report = self.0;

        let num_paths_not_looked_up = report
            .narinfo_lookup_errors
            .iter()
            .map(|&(num_paths, _)| num_paths)
            .sum::<u32>();

        let num_paths_failed = (report.path_closure_errors.len()
            + report.path_handling_errors.len())
            as u32;

        let num_paths_not_pushed = num_paths_failed + num_paths_not_looked_up;

        // Annotations are shown at the top of the workflow run, and here they
        // head the details below, since the job itself doesn't fail.

        if let Some(error) = &report.token_refresh_error {
            writeln!(
                f,
                "::warning::Couldn't refresh the token: {}",
                WithCauses::Inline(error),
            )?;
        }

        if num_paths_not_pushed > 0 {
            writeln!(
                f,
                "::warning title=See setup-cache's post step for \
                 details::Couldn't push {} to the cache",
                Count(num_paths_not_pushed, "store path"),
            )?;
        }

        if report.num_paths_skipped > 0 {
            writeln!(
                f,
                "Skipped {}",
                Count(report.num_paths_skipped, "store path")
            )?;
        }

        write!(f, "Pushed {}", Count(report.num_paths_pushed, "store path"))?;
        if report.num_paths_pushed > 0 {
            write!(
                f,
                ", totalling {}",
                HumanReadableByteSize(report.num_bytes_pushed)
            )?;
        }
        writeln!(f)?;

        for (num_paths, error) in &report.narinfo_lookup_errors {
            writeln!(
                f,
                "Failed to ask the cache which of {} it has: {}",
                Count(*num_paths, "store path"),
                WithCauses::Indented(error),
            )?;
        }

        if num_paths_failed > 0 {
            writeln!(
                f,
                "Failed to push {}:",
                Count(num_paths_failed, "store path")
            )?;

            let closure_errors = report
                .path_closure_errors
                .iter()
                .map(|(path, error)| (path, error as &dyn Error));

            let handling_errors = report
                .path_handling_errors
                .iter()
                .map(|(path, error)| (path, error as &dyn Error));

            for (path, error) in closure_errors.chain(handling_errors) {
                writeln!(f, "{path}: {}", WithCauses::Indented(error))?;
            }
        }

        Ok(())
    }
}

impl fmt::Display for WithCauses<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (Self::Inline(error) | Self::Indented(error)) = *self;

        write!(f, "{error}")?;

        let mut causes =
            iter::successors(error.source(), |&cause| cause.source());

        match self {
            Self::Inline(_) => {
                causes.try_for_each(|cause| write!(f, ": {cause}"))
            },
            Self::Indented(_) => {
                let num_causes = causes.clone().count();
                if num_causes > 0 {
                    write!(f, "\n  Caused by:")?;
                }
                causes.enumerate().try_for_each(|(idx, cause)| {
                    if num_causes == 1 {
                        write!(f, "\n    {cause}")
                    } else {
                        write!(f, "\n    {idx}: {cause}")
                    }
                })
            },
        }
    }
}

impl fmt::Display for Count<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(count, name) = *self;
        write!(f, "{count} {name}{}", if count == 1 { "" } else { "s" })
    }
}

struct HumanReadableByteSize(u64);

impl fmt::Display for HumanReadableByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB", "PiB"];

        let mut value = self.0 as f64;
        let mut unit_idx = 0;

        while unit_idx + 1 < UNITS.len() && value >= 1024.0 {
            value /= 1024.0;
            unit_idx += 1;
        }

        if unit_idx == 0 {
            write!(f, "{} {}", self.0, UNITS[unit_idx])
        } else {
            write!(f, "{value:.1} {}", UNITS[unit_idx])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_are_singular_only_for_one() {
        assert_eq!(Count(0, "store path").to_string(), "0 store paths");
        assert_eq!(Count(1, "store path").to_string(), "1 store path");
        assert_eq!(Count(21, "store path").to_string(), "21 store paths");
    }

    #[test]
    fn formats_binary_byte_sizes() {
        assert_eq!(HumanReadableByteSize(0).to_string(), "0 B");
        assert_eq!(HumanReadableByteSize(1023).to_string(), "1023 B");
        assert_eq!(HumanReadableByteSize(1024).to_string(), "1.0 KiB");
        assert_eq!(HumanReadableByteSize(1536).to_string(), "1.5 KiB");
        assert_eq!(HumanReadableByteSize(42690).to_string(), "41.7 KiB");
        assert_eq!(HumanReadableByteSize(1024 * 1024).to_string(), "1.0 MiB");
    }
}
