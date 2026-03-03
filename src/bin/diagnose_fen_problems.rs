use std::fs;
use std::io::{self, Read};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Parser;
use shakmaty::fen::Fen;
use shakmaty::{CastlingMode, Chess, PositionError};

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Diagnose invalid or suspicious FEN positions using shakmaty."
)]
struct Args {
    /// Input text file containing raw FENs or zobr collision lines.
    #[arg(long)]
    input: Option<PathBuf>,

    /// Raw input text directly on the command line.
    #[arg(long)]
    text: Option<String>,

    /// Also print positions that have no detected problems.
    #[arg(long, default_value_t = true)]
    include_ok: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let raw_input = load_raw_input(&args)?;
    let fens = extract_fens(&raw_input);

    if fens.is_empty() {
        bail!("no FEN-like positions found in the provided input");
    }

    println!("detected {} FENs", fens.len());
    for fen in fens {
        let problems = analyze_fen(&fen);
        if problems.is_empty() {
            if args.include_ok {
                println!("{fen}\tOK");
            }
            continue;
        }
        println!("{fen}\t{}", problems.join("; "));
    }

    Ok(())
}

fn load_raw_input(args: &Args) -> Result<String> {
    match (&args.input, &args.text) {
        (Some(_), Some(_)) => bail!("use either --input or --text, not both"),
        (Some(path), None) => fs::read_to_string(path)
            .with_context(|| format!("failed reading input file {}", path.display())),
        (None, Some(text)) => Ok(text.clone()),
        (None, None) => {
            let mut stdin = String::new();
            io::stdin()
                .read_to_string(&mut stdin)
                .context("failed to read stdin")?;
            if stdin.trim().is_empty() {
                bail!("stdin is empty; provide --input, --text, or pipe text to stdin");
            }
            Ok(stdin)
        }
    }
}

fn extract_fens(raw: &str) -> Vec<String> {
    let tokens: Vec<&str> = raw.split_whitespace().collect();
    let mut fens = Vec::new();
    let mut i = 0usize;

    while i < tokens.len() {
        if i + 3 < tokens.len()
            && looks_like_board_field(tokens[i])
            && looks_like_side_to_move(tokens[i + 1])
            && looks_like_castling_field(tokens[i + 2])
            && looks_like_en_passant_field(tokens[i + 3])
        {
            let mut fen = format!(
                "{} {} {} {}",
                tokens[i],
                tokens[i + 1],
                tokens[i + 2],
                tokens[i + 3]
            );
            i += 4;

            // Accept full 6-field FEN if both move counters are present.
            if i + 1 < tokens.len()
                && is_nonnegative_integer(tokens[i])
                && is_nonnegative_integer(tokens[i + 1])
            {
                fen.push(' ');
                fen.push_str(tokens[i]);
                fen.push(' ');
                fen.push_str(tokens[i + 1]);
                i += 2;
            }

            fens.push(fen);
            continue;
        }

        i += 1;
    }

    fens
}

fn analyze_fen(fen: &str) -> Vec<String> {
    let mut problems = Vec::new();

    let parsed = match Fen::from_ascii(fen.as_bytes()) {
        Ok(parsed) => parsed,
        Err(err) => {
            problems.push(format!("invalid fen syntax: {err}"));
            return problems;
        }
    };

    let mut pos_result: Result<Chess, PositionError<Chess>> =
        parsed.into_position(CastlingMode::Standard);

    pos_result = pos_result.or_else(|err| match err.ignore_invalid_castling_rights() {
        Ok(pos) => {
            problems.push("invalid castling rights".to_string());
            Ok(pos)
        }
        Err(err) => Err(err),
    });

    pos_result = pos_result.or_else(|err| match err.ignore_invalid_ep_square() {
        Ok(pos) => {
            problems.push("invalid en-passant square".to_string());
            Ok(pos)
        }
        Err(err) => Err(err),
    });

    pos_result = pos_result.or_else(|err| match err.ignore_impossible_check() {
        Ok(pos) => {
            problems.push("impossible check state".to_string());
            Ok(pos)
        }
        Err(err) => Err(err),
    });

    pos_result = pos_result.or_else(|err| match err.ignore_too_much_material() {
        Ok(pos) => {
            problems.push("too much material".to_string());
            Ok(pos)
        }
        Err(err) => Err(err),
    });

    if let Err(err) = pos_result {
        problems.push(format!("unrecoverable position error: {err}"));
    }

    problems
}

fn looks_like_board_field(token: &str) -> bool {
    let ranks: Vec<&str> = token.split('/').collect();
    if ranks.len() != 8 {
        return false;
    }

    ranks.iter().all(|rank| rank_has_eight_squares(rank))
}

fn rank_has_eight_squares(rank: &str) -> bool {
    if rank.is_empty() {
        return false;
    }

    let mut squares = 0usize;
    for ch in rank.chars() {
        match ch {
            '1'..='8' => squares += ch.to_digit(10).unwrap_or(0) as usize,
            'P' | 'N' | 'B' | 'R' | 'Q' | 'K' | 'p' | 'n' | 'b' | 'r' | 'q' | 'k' => squares += 1,
            _ => return false,
        }
    }

    squares == 8
}

fn looks_like_side_to_move(token: &str) -> bool {
    matches!(token, "w" | "b")
}

fn looks_like_castling_field(token: &str) -> bool {
    if token == "-" {
        return true;
    }

    if token.is_empty() || token.len() > 4 {
        return false;
    }

    let mut seen_k = false;
    let mut seen_q = false;
    let mut seen_big_k = false;
    let mut seen_big_q = false;

    for ch in token.chars() {
        match ch {
            'K' if !seen_big_k => seen_big_k = true,
            'Q' if !seen_big_q => seen_big_q = true,
            'k' if !seen_k => seen_k = true,
            'q' if !seen_q => seen_q = true,
            _ => return false,
        }
    }

    true
}

fn looks_like_en_passant_field(token: &str) -> bool {
    if token == "-" {
        return true;
    }

    let mut chars = token.chars();
    let Some(file) = chars.next() else {
        return false;
    };
    let Some(rank) = chars.next() else {
        return false;
    };
    if chars.next().is_some() {
        return false;
    }

    ('a'..='h').contains(&file) && ('1'..='8').contains(&rank)
}

fn is_nonnegative_integer(token: &str) -> bool {
    !token.is_empty() && token.chars().all(|c| c.is_ascii_digit())
}
