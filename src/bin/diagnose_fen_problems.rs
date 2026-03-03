use std::fs;
use std::io::{self, Read};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Parser;
use shakmaty::fen::Fen;
use shakmaty::{CastlingMode, Chess, Move, Position, PositionError};

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
}

#[derive(Clone, Debug)]
struct FenEntry {
    zobr64: Option<u64>,
    fen: String,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let raw_input = load_raw_input(&args)?;
    let entries = extract_fen_entries(&raw_input);

    if entries.is_empty() {
        bail!("no FEN-like positions found in the provided input");
    }

    let groups = group_entries_by_zobr64(entries);
    println!(
        "detected {} FENs in {} groups",
        groups.iter().map(|(_, fens)| fens.len()).sum::<usize>(),
        groups.len()
    );

    for (zobr64, fens) in groups {
        match zobr64 {
            Some(value) => println!("zobr64: {value} (fens={})", fens.len()),
            None => println!("zobr64: <unknown> (fens={})", fens.len()),
        }

        for fen in fens {
            let problems = analyze_fen(&fen);
            if problems.is_empty() {
                println!("{fen}\tOK");
            } else {
                println!("{fen}\t{}", problems.join("; "));
            }
        }
        println!();
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

fn extract_fen_entries(raw: &str) -> Vec<FenEntry> {
    let tokens: Vec<&str> = raw.split_whitespace().collect();
    let mut entries = Vec::new();
    let mut current_zobr64: Option<u64> = None;
    let mut i = 0usize;

    while i < tokens.len() {
        if tokens[i] == "zobr64:" {
            if i + 1 < tokens.len() {
                if let Ok(value) = tokens[i + 1].parse::<u64>() {
                    current_zobr64 = Some(value);
                    i += 2;
                    continue;
                }
            }
        }

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

            entries.push(FenEntry {
                zobr64: current_zobr64,
                fen,
            });
            continue;
        }

        i += 1;
    }

    entries
}

fn group_entries_by_zobr64(entries: Vec<FenEntry>) -> Vec<(Option<u64>, Vec<String>)> {
    let mut groups: Vec<(Option<u64>, Vec<String>)> = Vec::new();

    for entry in entries {
        if let Some((_, fens)) = groups.iter_mut().find(|(key, _)| *key == entry.zobr64) {
            fens.push(entry.fen);
        } else {
            groups.push((entry.zobr64, vec![entry.fen]));
        }
    }

    groups
}

fn analyze_fen(fen: &str) -> Vec<String> {
    let mut problems = Vec::new();
    let (board_field, side_to_move, castling_field, ep_field) = fen_aux_fields(fen);
    let mut had_invalid_castling_rights = false;

    if board_field == STARTING_BOARD_FEN && side_to_move == "b" {
        problems.push("impossible side to move for standard starting board".to_string());
    }

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
            had_invalid_castling_rights = true;
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

    match pos_result {
        Ok(position) => {
            let legal_moves = position.legal_moves();

            if castling_field != "-"
                && !had_invalid_castling_rights
                && !castling_rights_structurally_possible(board_field, castling_field)
            {
                problems.push("castling not possible from this position".to_string());
            }

            if ep_field != "-" {
                let has_legal_en_passant = legal_moves
                    .iter()
                    .any(|mv| matches!(mv, Move::EnPassant { .. }));
                if !has_legal_en_passant {
                    problems.push("en-passant not possible from this position".to_string());
                }
            }
        }
        Err(err) => {
            problems.push(format!("unrecoverable position error: {err}"));
        }
    }

    problems
}

const STARTING_BOARD_FEN: &str = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR";

fn fen_aux_fields(fen: &str) -> (&str, &str, &str, &str) {
    let mut fields = fen.split_whitespace();
    let board = fields.next().unwrap_or("");
    let side_to_move = fields.next().unwrap_or("");
    let castling = fields.next().unwrap_or("-");
    let en_passant = fields.next().unwrap_or("-");
    (board, side_to_move, castling, en_passant)
}

fn castling_rights_structurally_possible(board_field: &str, castling_field: &str) -> bool {
    for right in castling_field.chars() {
        let possible = match right {
            'K' => {
                board_piece_at(board_field, 'e', '1') == Some('K')
                    && board_piece_at(board_field, 'h', '1') == Some('R')
            }
            'Q' => {
                board_piece_at(board_field, 'e', '1') == Some('K')
                    && board_piece_at(board_field, 'a', '1') == Some('R')
            }
            'k' => {
                board_piece_at(board_field, 'e', '8') == Some('k')
                    && board_piece_at(board_field, 'h', '8') == Some('r')
            }
            'q' => {
                board_piece_at(board_field, 'e', '8') == Some('k')
                    && board_piece_at(board_field, 'a', '8') == Some('r')
            }
            _ => false,
        };

        if !possible {
            return false;
        }
    }

    true
}

fn board_piece_at(board_field: &str, file: char, rank: char) -> Option<char> {
    let file_idx = (file as u8).checked_sub(b'a')? as usize;
    if file_idx > 7 {
        return None;
    }

    let rank_num = rank.to_digit(10)? as usize;
    if !(1..=8).contains(&rank_num) {
        return None;
    }
    let rank_idx = 8 - rank_num;

    let ranks: Vec<&str> = board_field.split('/').collect();
    if ranks.len() != 8 {
        return None;
    }

    let mut cur_file = 0usize;
    for ch in ranks[rank_idx].chars() {
        if let Some(skip) = ch.to_digit(10) {
            cur_file += skip as usize;
            continue;
        }

        if cur_file == file_idx {
            return Some(ch);
        }
        cur_file += 1;
    }

    None
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
