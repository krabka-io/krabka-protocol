//! `sasl.kerberos.principal.to.local.rules` (`auth_to_local`) DSL.
//!
//! This module maps a Kerberos principal, `primary[/instance]@REALM`, to the
//! short name ACLs are written against. It follows Kafka's
//! `KerberosName`, `KerberosShortNamer` and `KerberosRule`: the same rule
//! grammar, the same `$n` format expansion, `java.util.regex` match and
//! replacement semantics through [`JavaPattern`], and the same failures. A
//! rule that fails, or that produces a name containing `/` or `@`, fails the
//! whole mapping rather than falling through to the next rule. The logic is
//! pure and needs no KDC.

use std::fmt;

use crate::java_regex::{JavaPattern, JavaReplacementError};

/// What a rule does to the case of its result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Case {
    /// No trailing flag.
    Preserve,
    /// `/L`.
    Lower,
    /// `/U`.
    Upper,
}

/// One `auth_to_local` rule.
#[derive(Debug, Clone)]
pub enum Rule {
    /// `DEFAULT`: when the principal's realm is the default realm, the result
    /// is the first component, however many components the principal has.
    Default,
    /// `RULE:[n:format](match)s/from/to/[g][/L|/U]`
    Translate(Box<Translate>),
}

/// A `RULE:[n:format](match)s/from/to/[g][/L|/U]` rule.
#[derive(Debug, Clone)]
pub struct Translate {
    /// The component count the principal must have.
    pub num_components: usize,
    /// The `$0` (realm), `$1`, `$2` template the rule matches against.
    pub format: String,
    /// `(match)`: the expanded format must match it as a whole.
    pub match_re: Option<JavaPattern>,
    /// `s/from/to/[g]`.
    pub subst: Option<Subst>,
    /// The trailing `L` or `U`.
    pub case: Case,
}

/// A rule's `s/from/to/[g]` substitution.
#[derive(Debug, Clone)]
pub struct Subst {
    from: JavaPattern,
    to: String,
    global: bool,
}

/// Why a rule does not parse, or a principal does not map.
///
/// The messages are those of the exceptions Kafka throws.
#[derive(Debug, thiserror::Error)]
pub enum NameError {
    /// The rule spec does not match Kafka's rule grammar.
    #[error("Invalid rule: {0}")]
    Parse(String),
    /// A rule's `(match)` or `s/from/` is not a regular expression.
    #[error("Invalid rule: {spec}: {source}")]
    Pattern {
        /// The rule as written.
        spec: String,
        /// Why the regular expression does not compile.
        source: regex::Error,
    },
    /// The principal has more than two components, or an empty-component
    /// structure `KerberosName` rejects.
    #[error("Malformed Kerberos name: {0}")]
    MalformedName(String),
    /// A `$` in a rule format is not followed by a component index in range.
    #[error("{0}")]
    BadFormat(String),
    /// A substitution's replacement is malformed.
    #[error("{0}")]
    Replacement(#[from] JavaReplacementError),
    /// A rule produced a name containing `/` or `@`.
    #[error("Non-simple name {name} after auth_to_local rule {rule}")]
    NonSimple {
        /// The rule's result.
        name: String,
        /// The rule, as Kafka prints it.
        rule: String,
    },
    /// No rule applies to the principal.
    #[error("No rules apply to {0}")]
    NoMatch(String),
}

impl Rule {
    /// Parses one rule the way `KerberosShortNamer.parseRules` does.
    ///
    /// The grammar is Kafka's `RULE_PARSER`, matched against the whole spec:
    /// `DEFAULT` or
    /// `RULE:[<digits>:<format>](<match>)s/<from>/<to>/[g][/][L|U]`, where the
    /// match, the substitution and the case flag are each optional, the
    /// substitution needs its closing `/`, and none of `format`, `match`,
    /// `from` and `to` can contain its own delimiter.
    ///
    /// # Errors
    ///
    /// [`NameError::Parse`] when the spec does not match the grammar and
    /// [`NameError::Pattern`] when a regular expression does not compile.
    pub fn parse(spec: &str) -> Result<Rule, NameError> {
        let spec = spec.trim();
        if spec == "DEFAULT" {
            return Ok(Rule::Default);
        }
        let invalid = || NameError::Parse(spec.to_owned());
        let pattern = |source: &str| {
            JavaPattern::new(source).map_err(|source| NameError::Pattern {
                spec: spec.to_owned(),
                source,
            })
        };
        let rest = spec.strip_prefix("RULE:[").ok_or_else(invalid)?;
        let digits = rest.len() - rest.trim_start_matches(|c: char| c.is_ascii_digit()).len();
        // `Integer.parseInt` of the `\d*` group rejects an empty count.
        let num_components: usize = rest[..digits].parse().map_err(|_| invalid())?;
        let rest = rest[digits..].strip_prefix(':').ok_or_else(invalid)?;
        let (format, mut rest) = rest.split_once(']').ok_or_else(invalid)?;

        let mut match_re = None;
        if let Some(after) = rest.strip_prefix('(')
            && let Some((source, after)) = after.split_once(')')
        {
            match_re = Some(pattern(source)?);
            rest = after;
        }

        let mut subst = None;
        if let Some(after) = rest.strip_prefix("s/")
            && let Some((from, after)) = after.split_once('/')
            && let Some((to, after)) = after.split_once('/')
        {
            let (global, after) = match after.strip_prefix('g') {
                Some(after) => (true, after),
                None => (false, after),
            };
            subst = Some(Subst {
                from: pattern(from)?,
                to: to.to_owned(),
                global,
            });
            rest = after;
        }

        let rest = rest.strip_prefix('/').unwrap_or(rest);
        let case = match rest {
            "" => Case::Preserve,
            "L" => Case::Lower,
            "U" => Case::Upper,
            _ => return Err(invalid()),
        };

        Ok(Rule::Translate(Box::new(Translate {
            num_components,
            format: format.to_owned(),
            match_re,
            subst,
            case,
        })))
    }

    /// `KerberosRule.apply`: the short name, `Ok(None)` when this rule does
    /// not apply, or the error that ends the mapping.
    ///
    /// `params` is `[realm, component, ...]`.
    fn apply(&self, params: &[&str], default_realm: &str) -> Result<Option<String>, NameError> {
        let (result, case) = match self {
            Rule::Default => (
                (params[0] == default_realm).then(|| params[1].to_owned()),
                Case::Preserve,
            ),
            Rule::Translate(translate) => {
                let Translate {
                    num_components,
                    format,
                    match_re,
                    subst,
                    case,
                } = &**translate;
                if params.len() - 1 != *num_components {
                    return Ok(None);
                }
                let base = replace_parameters(format, params)?;
                let result = if match_re.as_ref().is_none_or(|re| re.matches(&base)) {
                    Some(match subst {
                        None => base,
                        Some(Subst {
                            from,
                            to,
                            global: true,
                        }) => from.replace_all(&base, to)?,
                        Some(Subst {
                            from,
                            to,
                            global: false,
                        }) => from.replace_first(&base, to)?,
                    })
                } else {
                    None
                };
                (result, *case)
            }
        };
        let Some(result) = result else {
            return Ok(None);
        };
        if result.contains(['/', '@']) {
            return Err(NameError::NonSimple {
                name: result,
                rule: self.to_string(),
            });
        }
        Ok(Some(match case {
            Case::Preserve => result,
            Case::Lower => result.to_lowercase(),
            Case::Upper => result.to_uppercase(),
        }))
    }
}

/// Kafka's `KerberosRule.toString`, used in its error messages.
impl fmt::Display for Rule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Rule::Default => f.write_str("DEFAULT"),
            Rule::Translate(translate) => {
                let Translate {
                    num_components,
                    format,
                    match_re,
                    subst,
                    case,
                } = &**translate;
                write!(f, "RULE:[{num_components}:{format}]")?;
                if let Some(re) = match_re {
                    write!(f, "({})", re.as_str())?;
                }
                if let Some(Subst { from, to, global }) = subst {
                    write!(f, "s/{}/{to}/", from.as_str())?;
                    if *global {
                        f.write_str("g")?;
                    }
                }
                match case {
                    Case::Preserve => Ok(()),
                    Case::Lower => f.write_str("/L"),
                    Case::Upper => f.write_str("/U"),
                }
            }
        }
    }
}

/// `KerberosRule.replaceParameters`: `$0` is the realm and `$1`, `$2` the
/// components. A `$` without an index in range is an error.
fn replace_parameters(format: &str, params: &[&str]) -> Result<String, NameError> {
    let mut out = String::new();
    let mut rest = format;
    while let Some(dollar) = rest.find('$') {
        out.push_str(&rest[..dollar]);
        let after = &rest[dollar + 1..];
        let digits = after.len() - after.trim_start_matches(|c: char| c.is_ascii_digit()).len();
        let number = &after[..digits];
        let index: usize = number.parse().map_err(|_| {
            NameError::BadFormat(format!("bad format in username mapping in {number}"))
        })?;
        let param = params.get(index).ok_or_else(|| {
            NameError::BadFormat(format!(
                "index {index} from {format} is outside of the valid range 0 to {}",
                params.len() - 1
            ))
        })?;
        out.push_str(param);
        rest = &after[digits..];
    }
    out.push_str(rest);
    Ok(out)
}

/// A parsed Kerberos principal, Kafka's `KerberosName`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KerberosName {
    /// The first component.
    pub service_name: String,
    /// The second component, when there is one.
    pub host_name: Option<String>,
    /// The realm. `None` only for a name with no `@` at all.
    pub realm: Option<String>,
}

impl KerberosName {
    /// `KerberosName.parse`: `service[/host]@realm`, where no part contains
    /// `/` or `@`. A name without `@` is simple and has no host or realm.
    ///
    /// # Errors
    ///
    /// [`NameError::MalformedName`] when a name containing `@` is not of that
    /// shape, for example when it has three components.
    pub fn parse(principal: &str) -> Result<Self, NameError> {
        let malformed = || NameError::MalformedName(principal.to_owned());
        let Some((head, realm)) = principal.split_once('@') else {
            return Ok(Self {
                service_name: principal.to_owned(),
                host_name: None,
                realm: None,
            });
        };
        if realm.contains(['/', '@']) {
            return Err(malformed());
        }
        let (service_name, host_name) = match head.split_once('/') {
            Some((_, host)) if host.contains('/') => return Err(malformed()),
            Some((service, host)) => (service, Some(host.to_owned())),
            None => (head, None),
        };
        Ok(Self {
            service_name: service_name.to_owned(),
            host_name,
            realm: Some(realm.to_owned()),
        })
    }
}

impl fmt::Display for KerberosName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.service_name)?;
        if let Some(host) = &self.host_name {
            write!(f, "/{host}")?;
        }
        if let Some(realm) = &self.realm {
            write!(f, "@{realm}")?;
        }
        Ok(())
    }
}

/// `KerberosShortNamer.shortName`: applies `rules` in order, and the first
/// rule that applies gives the short name.
///
/// A name without a realm is already short and is returned as is.
/// `default_realm` is the realm `DEFAULT` strips. Kafka takes it from the
/// JVM's Kerberos configuration and uses the empty string when there is none,
/// in which case `DEFAULT` never applies to a principal with a realm.
///
/// # Errors
///
/// [`NameError::NoMatch`] when no rule applies, and the error of the first
/// rule that fails.
// auth_to_local principal mapping (Kerberos principal -> short ACL name).
// skip_all avoids capturing the compiled `rules`; the resolved short name is
// recorded in `mapped` on success. `err` surfaces a no-match or rule failure.
// One span per mapping, not per rule iteration.
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(mechanism = "GSSAPI", mapped = tracing::field::Empty),
    err
)]
pub fn short_name(
    rules: &[Rule],
    name: &KerberosName,
    default_realm: &str,
) -> Result<String, NameError> {
    let Some(realm) = name.realm.as_deref() else {
        return Ok(name.service_name.clone());
    };
    let mut params = vec![realm, name.service_name.as_str()];
    params.extend(name.host_name.as_deref());
    for rule in rules {
        if let Some(result) = rule.apply(&params, default_realm)? {
            tracing::Span::current().record("mapped", result.as_str());
            return Ok(result);
        }
    }
    let rules = rules.iter().map(ToString::to_string).collect::<Vec<_>>();
    Err(NameError::NoMatch(format!(
        "{name}, rules [{}]",
        rules.join(", ")
    )))
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    fn rules(specs: &[&str]) -> Vec<Rule> {
        specs
            .iter()
            .map(|s| Rule::parse(s).unwrap_or_else(|e| panic!("{s}: {e}")))
            .collect()
    }

    /// Rules, default realm, principal, and the short name or error message.
    type MappingCase<'a> = (&'a [&'a str], &'a str, &'a str, Result<&'a str, &'a str>);

    fn map(specs: &[&str], default_realm: &str, principal: &str) -> Result<String, String> {
        let name = KerberosName::parse(principal).map_err(|e| e.to_string())?;
        short_name(&rules(specs), &name, default_realm).map_err(|e| e.to_string())
    }

    /// Kafka's `KerberosNameTest.testParse`, `testToLowerCase` and
    /// `testToUpperCase`, plus the cases the compatibility audit raised. Every
    /// row was checked against `KerberosShortNamer` from kafka-clients 4.3.1.
    #[test]
    fn short_names_follow_kafka() {
        const TEST_PARSE: &[&str] = &[
            r"RULE:[1:$1](App\..*)s/App\.(.*)/$1/g",
            r"RULE:[2:$1](App\..*)s/App\.(.*)/$1/g",
            "DEFAULT",
        ];
        const TO_LOWER: &[&str] = &[
            "RULE:[1:$1]/L",
            "RULE:[2:$1](Test.*)s/ABC///L",
            "RULE:[2:$1](ABC.*)s/ABC/XYZ/g/L",
            r"RULE:[2:$1](App\..*)s/App\.(.*)/$1/g/L",
            "RULE:[2:$1]/L",
            "DEFAULT",
        ];
        const TO_UPPER: &[&str] = &[
            "RULE:[1:$1]/U",
            "RULE:[2:$1](Test.*)s/ABC///U",
            "RULE:[2:$1](ABC.*)s/ABC/XYZ/g/U",
            r"RULE:[2:$1](App\..*)s/App\.(.*)/$1/g/U",
            "RULE:[2:$1]/U",
            "DEFAULT",
        ];
        const AUDIT: &[&str] = &[
            "RULE:[1:$1](ali)s/x/y/",
            "RULE:[2:$1](.*)s/(.*)/$1_$1/U",
            "RULE:[1:$1@$0](.*@OTHER)s/@.*//",
            "RULE:[1:$1](bad.*)s/a/@/",
            "DEFAULT",
        ];
        // (rules, default realm, principal, short name or error)
        let cases: &[MappingCase<'_>] = &[
            (
                TEST_PARSE,
                "REALM.COM",
                "App.service-name/example.com@REALM.COM",
                Ok("service-name"),
            ),
            (
                TEST_PARSE,
                "REALM.COM",
                "App.service-name@REALM.COM",
                Ok("service-name"),
            ),
            // DEFAULT strips the default realm whatever the component count.
            (TEST_PARSE, "REALM.COM", "user/host@REALM.COM", Ok("user")),
            (
                &["DEFAULT"],
                "REALM",
                "kafka/host.example.com@REALM",
                Ok("kafka"),
            ),
            (TO_LOWER, "REALM.COM", "User@REALM.COM", Ok("user")),
            (TO_LOWER, "REALM.COM", "TestABC/host@FOO.COM", Ok("test")),
            (
                TO_LOWER,
                "REALM.COM",
                "ABC_User_ABC/host@FOO.COM",
                Ok("xyz_user_xyz"),
            ),
            (
                TO_LOWER,
                "REALM.COM",
                "App.SERVICE-name/example.com@REALM.COM",
                Ok("service-name"),
            ),
            (TO_LOWER, "REALM.COM", "User/root@REALM.COM", Ok("user")),
            (TO_UPPER, "REALM.COM", "User@REALM.COM", Ok("USER")),
            (TO_UPPER, "REALM.COM", "TestABC/host@FOO.COM", Ok("TEST")),
            (
                TO_UPPER,
                "REALM.COM",
                "ABC_User_ABC/host@FOO.COM",
                Ok("XYZ_USER_XYZ"),
            ),
            (
                TO_UPPER,
                "REALM.COM",
                "App.SERVICE-name/example.com@REALM.COM",
                Ok("SERVICE-NAME"),
            ),
            (TO_UPPER, "REALM.COM", "User/root@REALM.COM", Ok("USER")),
            // `(ali)` is a whole-string match, so it does not apply to alice.
            (AUDIT, "REALM.COM", "alice@REALM.COM", Ok("alice")),
            (AUDIT, "REALM.COM", "ali@REALM.COM", Ok("ali")),
            // `$1_` is group 1 then `_`, and `/U` uppercases.
            (AUDIT, "REALM.COM", "svc/host@REALM.COM", Ok("SVC_SVC")),
            (AUDIT, "REALM.COM", "bob@OTHER", Ok("bob")),
            // A non-simple result ends the mapping; DEFAULT is not tried.
            (
                AUDIT,
                "REALM.COM",
                "bad@REALM.COM",
                Err("Non-simple name b@d after auth_to_local rule RULE:[1:$1](bad.*)s/a/@/"),
            ),
            (
                AUDIT,
                "REALM.COM",
                "u@ELSE",
                Err("No rules apply to u@ELSE, rules [RULE:[1:$1](ali)s/x/y/, \
                     RULE:[2:$1](.*)s/(.*)/$1_$1//U, RULE:[1:$1@$0](.*@OTHER)s/@.*//, \
                     RULE:[1:$1](bad.*)s/a/@/, DEFAULT]"),
            ),
            (
                AUDIT,
                "REALM.COM",
                "x/y/z@REALM.COM",
                Err("Malformed Kerberos name: x/y/z@REALM.COM"),
            ),
            // A name without a realm is already short.
            (AUDIT, "REALM.COM", "simple", Ok("simple")),
            // With no default realm, DEFAULT never strips a realm.
            (
                &["DEFAULT"],
                "",
                "x@REALM",
                Err("No rules apply to x@REALM, rules [DEFAULT]"),
            ),
            (&["DEFAULT"], "", "x@", Ok("x")),
            // A format index out of range fails the mapping.
            (
                &["RULE:[1:$2]", "DEFAULT"],
                "R",
                "x@R",
                Err("index 2 from $2 is outside of the valid range 0 to 1"),
            ),
            (
                &["RULE:[1:$]"],
                "R",
                "x@R",
                Err("bad format in username mapping in "),
            ),
            (&["RULE:[1:$1@$0]s/@R$//"], "R", "x@R", Ok("x")),
        ];
        for (specs, default_realm, principal, expected) in cases {
            check!(
                map(specs, default_realm, principal).as_deref()
                    == expected.map_err(ToString::to_string).as_deref(),
                "{principal} under {specs:?}"
            );
        }
    }

    /// Kafka's `KerberosNameTest.testInvalidRules`, and the substitution
    /// without its closing slash that `RULE_PARSER` leaves unmatched.
    #[test]
    fn invalid_rules_are_rejected() {
        for spec in [
            "default",
            "DEFAUL",
            "DEFAULT/L",
            "DEFAULT/g",
            "rule:[1:$1]",
            "rule:[1:$1]/L/U",
            "rule:[1:$1]/U/L",
            "rule:[1:$1]/LU",
            "RULE:[1:$1/L",
            "RULE:[1:$1]/l",
            "RULE:[2:$1](ABC.*)s/ABC/XYZ/L/g",
            "RULE:[1:$1]s/A/a",
            "RULE:[:$1]",
            "RULE:[1:$1](a",
        ] {
            check!(Rule::parse(spec).is_err(), "{spec}");
        }
    }

    #[test]
    fn rules_print_as_kafka_prints_them() {
        for (spec, printed) in [
            ("DEFAULT", "DEFAULT"),
            (
                "RULE:[2:$1@$0](.*@R)s/@R//gL",
                "RULE:[2:$1@$0](.*@R)s/@R//g/L",
            ),
            ("RULE:[1:$1]U", "RULE:[1:$1]/U"),
        ] {
            check!(rules(&[spec])[0].to_string() == printed);
        }
    }

    #[test]
    fn names_parse_as_kerberos_name_does() {
        let name = |service: &str, host: Option<&str>, realm: Option<&str>| KerberosName {
            service_name: service.to_owned(),
            host_name: host.map(ToOwned::to_owned),
            realm: realm.map(ToOwned::to_owned),
        };
        check!(KerberosName::parse("a/b@R").ok() == Some(name("a", Some("b"), Some("R"))));
        check!(KerberosName::parse("a@R").ok() == Some(name("a", None, Some("R"))));
        check!(KerberosName::parse("a/b").ok() == Some(name("a/b", None, None)));
        check!(KerberosName::parse("a@R@S").is_err());
    }
}
