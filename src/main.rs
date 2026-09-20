use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;

use avrocheck::compat::{check, Issue};
use avrocheck::schema::parse_fields;
use clap::Parser;

#[derive(Parser)]
#[command(
    name = "avrocheck",
    about = "Checks two Avro schema versions for backward/forward compatibility"
)]
struct Cli {
    /// The older (currently deployed) schema file.
    old_schema: PathBuf,
    /// The newer (proposed) schema file.
    new_schema: PathBuf,
}

fn print_issues(direction: &str, ok: bool, issues: &[Issue]) {
    if ok {
        println!("{direction}: compatible");
    } else {
        println!("{direction}: INCOMPATIBLE");
        for issue in issues {
            println!("  - {}: {}", issue.field, issue.reason);
        }
    }
}

fn main() -> anyhow::Result<ExitCode> {
    let cli = Cli::parse();
    let old_content = fs::read_to_string(&cli.old_schema)
        .map_err(|e| anyhow::anyhow!("reading {}: {e}", cli.old_schema.display()))?;
    let new_content = fs::read_to_string(&cli.new_schema)
        .map_err(|e| anyhow::anyhow!("reading {}: {e}", cli.new_schema.display()))?;

    let old_fields = parse_fields(&old_content)
        .map_err(|e| anyhow::anyhow!("{}: {e}", cli.old_schema.display()))?;
    let new_fields = parse_fields(&new_content)
        .map_err(|e| anyhow::anyhow!("{}: {e}", cli.new_schema.display()))?;

    let report = check(&old_fields, &new_fields);
    print_issues(
        "backward compatibility (new schema reading old data)",
        report.backward_compatible,
        &report.backward_issues,
    );
    print_issues(
        "forward compatibility  (old schema reading new data)",
        report.forward_compatible,
        &report.forward_issues,
    );

    if report.backward_compatible && report.forward_compatible {
        Ok(ExitCode::SUCCESS)
    } else {
        Ok(ExitCode::from(1))
    }
}
