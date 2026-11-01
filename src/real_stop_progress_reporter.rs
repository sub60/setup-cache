use core::error::Error as _;
use core::fmt::{self, Write};
use core::num::NonZeroU64;

use either::Either;
use futures::{AsyncWrite, AsyncWriteExt};
use nix_types::StorePath;

use crate::protocol::StoreDir;
use crate::{context, event_loop};

pub(crate) struct RealStopProgressReporter<W> {
    reported_paths_left_to_handle: bool,
    writer: W,
}

impl<W> RealStopProgressReporter<W> {
    pub(crate) fn new(writer: W) -> Self {
        Self { reported_paths_left_to_handle: false, writer }
    }
}

impl<W: AsyncWrite + Unpin> context::StopProgressReporter
    for RealStopProgressReporter<W>
{
    async fn report_paths_left_to_handle(
        &mut self,
        num_paths_left_to_handle: u32,
    ) {
        let (verb, plural) = match num_paths_left_to_handle {
            0 => return,
            1 => ("is", ""),
            _ => ("are", "s"),
        };
        let msg = format!(
            "There {verb} still {num_paths_left_to_handle} store path{plural} \
             left to handle\n"
        );
        let _ = self.writer.write_all(msg.as_bytes()).await;
        self.reported_paths_left_to_handle = true;
    }

    async fn report_path_pushed(
        &mut self,
        path: &StorePath<StoreDir>,
        _: NonZeroU64,
    ) {
        let msg = format!("Pushed {path}\n",);
        let _ = self.writer.write_all(msg.as_bytes()).await;
    }

    async fn report_path_skipped(&mut self, path: &StorePath<StoreDir>) {
        let msg = format!("Skipped {path}\n",);
        let _ = self.writer.write_all(msg.as_bytes()).await;
    }

    async fn report_path_handling_error<C: context::Cache, N: context::Nix>(
        &mut self,
        path: &StorePath<StoreDir>,
        _error: &event_loop::HandlePathError<C, N>,
    ) {
        let msg = format!("Failed handling {path}\n",);
        let _ = self.writer.write_all(msg.as_bytes()).await;
    }

    async fn report_final_report<Ctx: context::Context>(
        mut self,
        report: event_loop::ActionReport<Ctx>,
    ) {
        let mut msg = String::default();

        if self.reported_paths_left_to_handle {
            msg.write_char('\n').expect("never fails");
        }

        writeln!(
            &mut msg,
            "{} store path{} {} skipped",
            report.num_paths_skipped,
            if report.num_paths_skipped == 1 { "" } else { "s" },
            if report.num_paths_skipped == 1 { "was" } else { "were" },
        )
        .expect("never fails");

        write!(
            &mut msg,
            "{} store path{} {} pushed",
            report.num_paths_pushed,
            if report.num_paths_pushed == 1 { "" } else { "s" },
            if report.num_paths_pushed == 1 { "was" } else { "were" },
        )
        .expect("never fails");

        if report.num_paths_pushed > 0 {
            writeln!(
                &mut msg,
                ", totalling {}",
                HumanReadableByteSize(report.num_bytes_pushed)
            )
        } else {
            writeln!(&mut msg)
        }
        .expect("never fails");

        // Warnings are shown as annotations on the workflow run, since the job
        // itself doesn't fail.

        if let Some(error) = &report.token_error {
            write!(
                &mut msg,
                "::warning::Couldn't get a token to push store paths with: \
                 {error}"
            )
            .expect("never fails");

            // Annotations are a single line, so the causes are joined with
            // colons.
            let mut source = error.source();
            while let Some(err) = source {
                write!(&mut msg, ": {err}").expect("never fails");
                source = err.source();
            }

            msg.write_char('\n').expect("never fails");
        }

        let num_errors = report.path_closure_errors.len()
            + report.path_handling_errors.len();

        let num_not_pushed =
            num_errors + report.num_paths_without_token as usize;

        if num_not_pushed > 0 {
            writeln!(
                &mut msg,
                "::warning::{num_not_pushed} store path{} {} pushed to the \
                 cache, see the logs of setup-cache's post step",
                if num_not_pushed == 1 { "" } else { "s" },
                if num_not_pushed == 1 { "wasn't" } else { "weren't" },
            )
            .expect("never fails");
        }

        if report.num_paths_without_token > 0 {
            writeln!(
                &mut msg,
                "{} store path{} {} handled because there was no token to \
                 push {} with",
                report.num_paths_without_token,
                if report.num_paths_without_token == 1 { "" } else { "s" },
                if report.num_paths_without_token == 1 {
                    "wasn't"
                } else {
                    "weren't"
                },
                if report.num_paths_without_token == 1 { "it" } else { "them" },
            )
            .expect("never fails");
        }

        if num_errors > 0 {
            writeln!(
                &mut msg,
                "{num_errors} store path{} could not be handled:",
                if num_errors == 1 { "" } else { "s" },
            )
            .expect("never fails");

            let errors = report
                .path_closure_errors
                .into_iter()
                .map(|(path, err)| (path, Either::Left(err)))
                .chain(
                    report
                        .path_handling_errors
                        .into_iter()
                        .map(|(path, err)| (path, Either::Right(err))),
                );

            for (store_path, error) in errors {
                write!(&mut msg, "{store_path}: {error}").expect("never fails");

                let mut source = error.source();
                while let Some(err) = source {
                    write!(&mut msg, "\n  caused by: {err}")
                        .expect("never fails");
                    source = err.source();
                }

                msg.write_char('\n').expect("never fails");
            }
        }

        let _ = self.writer.write_all(msg.as_bytes()).await;
        let _ = self.writer.flush().await;
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
    fn formats_binary_byte_sizes() {
        assert_eq!(HumanReadableByteSize(0).to_string(), "0 B");
        assert_eq!(HumanReadableByteSize(1023).to_string(), "1023 B");
        assert_eq!(HumanReadableByteSize(1024).to_string(), "1.0 KiB");
        assert_eq!(HumanReadableByteSize(1536).to_string(), "1.5 KiB");
        assert_eq!(HumanReadableByteSize(42690).to_string(), "41.7 KiB");
        assert_eq!(HumanReadableByteSize(1024 * 1024).to_string(), "1.0 MiB");
    }
}
