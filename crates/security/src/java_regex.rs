//! `java.util.regex` match and replacement semantics over the `regex` crate.
//!
//! Kafka's principal-mapping rules, `sasl.kerberos.principal.to.local.rules`
//! and `ssl.principal.mapping.rules`, are Java regular expressions applied
//! with `Matcher.matches()`, `Matcher.replaceAll` and `Matcher.replaceFirst`.
//! The `regex` crate differs from Java in the three places those rules touch:
//!
//! - **Whole-input match.** `Matcher.matches()` succeeds when *some* match of
//!   the pattern spans the whole input, found by backtracking. A leftmost-first
//!   `find` that happens to stop short, as `a|ab` does on `ab`, is not the
//!   same test. [`JavaPattern::matches`] runs a separately compiled
//!   `\A(?:pattern)\z`.
//! - **Group references in the replacement.** Java reads `$` then one digit,
//!   and extends the number digit by digit only while the result is still a
//!   group of the pattern, so `$1_$2` is group 1, `_`, group 2 and `$12` with
//!   one group is group 1 then `2`. The `regex` crate takes the longest
//!   `[_0-9A-Za-z]` run as the group name and expands an unknown group to
//!   nothing. A backslash escapes the next character, and malformed
//!   references are errors rather than empty text.
//! - **Empty matches during `replaceAll`.** Java retries one character past an
//!   empty match and allows an empty match right after a non-empty one, so
//!   `x*` over `xx` with `[$0]` is `[xx][]`.
//!
//! - **Predefined classes.** Without `UNICODE_CHARACTER_CLASS`, Java's `\d`,
//!   `\w`, `\s` and `\b` are ASCII-only; the `regex` crate's are Unicode.
//!   [`JavaPattern::new`] rewrites them to their ASCII forms. It also makes
//!   any escaped non-alphanumeric character a literal, as Java does, where
//!   the `regex` crate rejects an escaped `,` or `@`.
//!
//! Otherwise the pattern dialect is the `regex` crate's: constructs it does
//! not support, such as back-references and look-around, fail to compile.

use regex::{Captures, Regex};

/// Why a Java replacement string could not be expanded against a match.
///
/// Each variant is one of the `IllegalArgumentException` or
/// `IndexOutOfBoundsException` messages `Matcher.appendReplacement` throws.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JavaReplacementError {
    /// A trailing `\` with nothing after it.
    #[error("character to be escaped is missing")]
    MissingEscapedCharacter,
    /// A trailing `$` with nothing after it.
    #[error("Illegal group reference: group index is missing")]
    MissingGroupIndex,
    /// A `$` followed by something other than a digit or `{`.
    #[error("Illegal group reference")]
    IllegalGroupReference,
    /// `${}`.
    #[error("named capturing group has 0 length name")]
    EmptyGroupName,
    /// `${name` with no closing brace.
    #[error("named capturing group is missing trailing '}}'")]
    UnterminatedGroupName,
    /// `${1a}`: a group name has to start with a letter.
    #[error("capturing group name {{{0}}} starts with digit character")]
    GroupNameStartsWithDigit(String),
    /// `${name}` where the pattern has no group of that name.
    #[error("No group with name {{{0}}}")]
    NoGroupNamed(String),
    /// `$n` where the pattern has fewer than `n` groups.
    #[error("No group {0}")]
    NoGroup(usize),
}

/// A pattern compiled for Java's `matches`, `replaceAll` and `replaceFirst`.
#[derive(Debug, Clone)]
pub struct JavaPattern {
    source: String,
    unanchored: Regex,
    whole: Regex,
}

impl JavaPattern {
    /// Compiles `pattern`.
    ///
    /// # Errors
    ///
    /// [`regex::Error`] when `pattern` is not a valid `regex` crate pattern.
    pub fn new(pattern: &str) -> Result<Self, regex::Error> {
        let translated = ascii_predefined_classes(pattern);
        Ok(Self {
            source: pattern.to_owned(),
            unanchored: Regex::new(&translated)?,
            whole: Regex::new(&format!(r"\A(?:{translated})\z"))?,
        })
    }

    /// The pattern as written.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.source
    }

    /// Java's `Matcher.groupCount()`: the capturing groups, not counting the
    /// whole match.
    #[must_use]
    pub fn group_count(&self) -> usize {
        self.unanchored.captures_len() - 1
    }

    /// Java's `Matcher.matches()`: whether some match spans all of `text`.
    #[must_use]
    pub fn matches(&self, text: &str) -> bool {
        self.whole.is_match(text)
    }

    /// Java's `Matcher.replaceAll(replacement)`.
    ///
    /// # Errors
    ///
    /// [`JavaReplacementError`] when `replacement` is malformed. As in Java,
    /// that is only found out when the pattern matches somewhere.
    pub fn replace_all(
        &self,
        text: &str,
        replacement: &str,
    ) -> Result<String, JavaReplacementError> {
        self.replace(text, replacement, usize::MAX)
    }

    /// Java's `Matcher.replaceFirst(replacement)`.
    ///
    /// # Errors
    ///
    /// As [`JavaPattern::replace_all`].
    pub fn replace_first(
        &self,
        text: &str,
        replacement: &str,
    ) -> Result<String, JavaReplacementError> {
        self.replace(text, replacement, 1)
    }

    fn replace(
        &self,
        text: &str,
        replacement: &str,
        limit: usize,
    ) -> Result<String, JavaReplacementError> {
        let mut out = String::with_capacity(text.len());
        let mut appended = 0;
        let mut search = 0;
        let mut replaced = 0;
        while replaced < limit && search <= text.len() {
            let Some(captures) = self.unanchored.captures_at(text, search) else {
                break;
            };
            let whole = captures.get(0).expect("group 0 always participates");
            out.push_str(&text[appended..whole.start()]);
            self.expand(&captures, replacement, &mut out)?;
            appended = whole.end();
            replaced += 1;
            // `Matcher.find` resumes at the previous end, one character
            // further when the previous match was empty.
            search = if whole.is_empty() {
                match text[whole.end()..].chars().next() {
                    Some(next) => whole.end() + next.len_utf8(),
                    None => break,
                }
            } else {
                whole.end()
            };
        }
        out.push_str(&text[appended..]);
        Ok(out)
    }

    /// `Matcher.appendExpandedReplacement`.
    fn expand(
        &self,
        captures: &Captures<'_>,
        replacement: &str,
        out: &mut String,
    ) -> Result<(), JavaReplacementError> {
        let group_count = self.group_count();
        let mut chars = replacement.chars().peekable();
        while let Some(character) = chars.next() {
            match character {
                '\\' => out.push(
                    chars
                        .next()
                        .ok_or(JavaReplacementError::MissingEscapedCharacter)?,
                ),
                '$' => {
                    let group = match chars.next() {
                        None => return Err(JavaReplacementError::MissingGroupIndex),
                        Some('{') => {
                            let mut name = String::new();
                            while let Some(&next) = chars.peek()
                                && next.is_ascii_alphanumeric()
                            {
                                name.push(next);
                                chars.next();
                            }
                            if name.is_empty() {
                                return Err(JavaReplacementError::EmptyGroupName);
                            }
                            if chars.next() != Some('}') {
                                return Err(JavaReplacementError::UnterminatedGroupName);
                            }
                            if name.starts_with(|c: char| c.is_ascii_digit()) {
                                return Err(JavaReplacementError::GroupNameStartsWithDigit(name));
                            }
                            self.unanchored
                                .capture_names()
                                .position(|candidate| candidate == Some(name.as_str()))
                                .ok_or(JavaReplacementError::NoGroupNamed(name))?
                        }
                        Some(first) => {
                            let mut group = first
                                .to_digit(10)
                                .ok_or(JavaReplacementError::IllegalGroupReference)?
                                as usize;
                            // "Capture the largest legal group string."
                            while let Some(digit) = chars.peek().and_then(|c| c.to_digit(10)) {
                                let longer = group * 10 + digit as usize;
                                if longer > group_count {
                                    break;
                                }
                                group = longer;
                                chars.next();
                            }
                            if group > group_count {
                                return Err(JavaReplacementError::NoGroup(group));
                            }
                            group
                        }
                    };
                    if let Some(text) = captures.get(group) {
                        out.push_str(text.as_str());
                    }
                }
                literal => out.push(literal),
            }
        }
        Ok(())
    }
}

/// Rewrites Java's ASCII-only predefined classes and word boundaries into
/// `regex` crate syntax with the same meaning. Each class becomes a bracketed
/// class, which the `regex` crate also accepts nested inside another class.
fn ascii_predefined_classes(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len());
    let mut class_depth = 0_usize;
    let mut chars = pattern.chars();
    while let Some(character) = chars.next() {
        match character {
            '\\' => {
                let Some(escaped) = chars.next() else {
                    out.push('\\');
                    break;
                };
                match escaped {
                    'd' => out.push_str("[0-9]"),
                    'D' => out.push_str("[^0-9]"),
                    'w' => out.push_str("[0-9A-Za-z_]"),
                    'W' => out.push_str("[^0-9A-Za-z_]"),
                    's' => out.push_str(r"[\t\n\x0B\f\r ]"),
                    'S' => out.push_str(r"[^\t\n\x0B\f\r ]"),
                    'b' if class_depth == 0 => out.push_str(r"(?-u:\b)"),
                    'B' if class_depth == 0 => out.push_str(r"(?-u:\B)"),
                    // Java takes any escaped non-alphanumeric character
                    // literally; the `regex` crate only accepts escaped
                    // metacharacters.
                    other if !other.is_ascii_alphanumeric() => {
                        out.push_str(&regex::escape(other.encode_utf8(&mut [0; 4])));
                    }
                    other => {
                        out.push('\\');
                        out.push(other);
                    }
                }
            }
            '[' => {
                class_depth += 1;
                out.push('[');
            }
            ']' => {
                class_depth = class_depth.saturating_sub(1);
                out.push(']');
            }
            other => out.push(other),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// Each row was checked against `java.util.regex` on `OpenJDK` 21:
    /// `Pattern.compile(pattern).matcher(text).replaceAll(replacement)`.
    #[test]
    fn replace_all_follows_java() {
        // (pattern, text, replacement, expected)
        let cases: &[(&str, &str, &str, Result<&str, JavaReplacementError>)] = &[
            // `$1_` is group 1 then `_`, not a group named `1_`.
            ("(a)(b)", "ab", "$1_$2", Ok("a_b")),
            ("(a)(b)", "ab", "$1suffix", Ok("asuffix")),
            // `$12` with two groups is group 1 then a literal `2`.
            ("(a)(b)", "ab", "$12", Ok("a2")),
            // With eleven groups `$11` is group 11 and `$111` is group 11 then `1`.
            (
                "(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)",
                "abcdefghijk",
                "$11,$111,$1a",
                Ok("k,k1,aa"),
            ),
            ("(a)(b)", "ab", "$3", Err(JavaReplacementError::NoGroup(3))),
            ("(a)(b)", "ab", r"\$1", Ok("$1")),
            ("(a)", "a", r"x\\", Ok(r"x\")),
            ("(a)", "a", r"x\/y", Ok("x/y")),
            (
                "(a)",
                "a",
                "x$",
                Err(JavaReplacementError::MissingGroupIndex),
            ),
            (
                "(a)",
                "a",
                r"x\",
                Err(JavaReplacementError::MissingEscapedCharacter),
            ),
            (
                "(a)",
                "a",
                "$x",
                Err(JavaReplacementError::IllegalGroupReference),
            ),
            ("(?<nm>a)(b)", "ab", "${nm}-$2", Ok("a-b")),
            ("(a)", "a", "${}", Err(JavaReplacementError::EmptyGroupName)),
            (
                "(a)",
                "a",
                "${nm",
                Err(JavaReplacementError::UnterminatedGroupName),
            ),
            (
                "(a)",
                "a",
                "${1a}",
                Err(JavaReplacementError::GroupNameStartsWithDigit(
                    "1a".to_owned(),
                )),
            ),
            (
                "(a)",
                "a",
                "${zz}",
                Err(JavaReplacementError::NoGroupNamed("zz".to_owned())),
            ),
            // A group that did not participate expands to nothing.
            ("(a)|(b)", "b", "[$1]", Ok("[]")),
            // Leftmost-first alternation, then the scan resumes after `a`.
            ("a|ab", "ab", "X", Ok("Xb")),
            // An empty match right after a non-empty one is still replaced.
            ("x*", "xx", "[$0]", Ok("[xx][]")),
            ("x*", "", "-", Ok("-")),
            ("", "ab", "-", Ok("-a-b-")),
            ("a*", "baaa", "X", Ok("XbXX")),
            // No match: a malformed replacement is never looked at.
            ("z", "ab", "$", Ok("ab")),
        ];
        for (pattern, text, replacement, expected) in cases {
            let compiled = JavaPattern::new(pattern).expect("pattern compiles");
            check!(
                compiled.replace_all(text, replacement).as_deref() == expected.as_deref(),
                "{pattern} over {text} with {replacement}"
            );
        }
    }

    #[test]
    fn replace_first_stops_after_one_match() {
        let compiled = JavaPattern::new("a").expect("pattern compiles");
        check!(compiled.replace_first("banana", "o").as_deref() == Ok("bonana"));
    }

    /// `Matcher.matches()` backtracks to any whole-input match, which a
    /// leftmost-first `find` can miss.
    #[test]
    fn matches_is_a_whole_input_match() {
        // (pattern, text, matches)
        let cases = [
            ("a|ab", "ab", true),
            ("CN=(.*?)", "CN=alice", true),
            ("CN=(.*?),", "CN=alice,OU=x", false),
            ("ali", "alice", false),
            ("^DC=(a)|(b)$", "DC=b", false),
            ("", "", true),
            // Java's predefined classes are ASCII-only.
            (r"CN=(\d+)", "CN=12", true),
            (r"CN=(\d+)", "CN=\u{663}", false),
            (r"\w+", "\u{e9}", false),
            (r"\W", "\u{e9}", true),
            (r"\s", "\u{a0}", false),
            (r"[\d\s]+", "1 2", true),
            (r"a\b.", "a\u{e9}", true),
            (r"\[\d\]", "[1]", true),
            // Escaped punctuation is literal, in and out of a class.
            (r"CN=a\,b\@c\/d", "CN=a,b@c/d", true),
            (r"[\,\]\-]+", ",]-", true),
            (r"\\,", r"\,", true),
        ];
        for (pattern, text, expected) in cases {
            let compiled = JavaPattern::new(pattern).expect("pattern compiles");
            check!(
                compiled.matches(text) == expected,
                "{pattern} against {text}"
            );
        }
    }

    #[test]
    fn group_count_excludes_the_whole_match() {
        check!(
            JavaPattern::new("(a)(?:b)(?<c>c)")
                .expect("compiles")
                .group_count()
                == 2
        );
    }
}
