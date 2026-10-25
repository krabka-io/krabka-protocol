//! Certificate-chain verification through the rustls configs that
//! [`TlsConfig`] builds.
//!
//! Each case issues a fresh PKI with rcgen and runs a real TLS handshake in
//! memory, so the verdict comes from rustls and rustls-webpki, not from a
//! config builder's own checks. A leaf whose chain runs through an intermediate
//! that an untrusted root signed is rejected, as is a leaf whose intermediate
//! is missing, and a leaf that chains to the trusted root is accepted. The
//! client configs check the server's chain, and the `Required` server config
//! checks the client's.

use std::{path::PathBuf, sync::Arc};

use krabka_security::{ClientAuthMode, TlsConfig};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use rustls::{
    CertificateError, ClientConfig, ClientConnection, ServerConfig, ServerConnection,
    pki_types::ServerName,
};

const SERVER_NAME: &str = "localhost";

type Issuer = CertifiedIssuer<'static, KeyPair>;

fn ca_params(common_name: &str) -> CertificateParams {
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("CA params");
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    params
}

fn root(common_name: &str) -> Issuer {
    CertifiedIssuer::self_signed(ca_params(common_name), KeyPair::generate().expect("key"))
        .expect("root")
}

fn intermediate(common_name: &str, parent: &Issuer) -> Issuer {
    CertifiedIssuer::signed_by(
        ca_params(common_name),
        KeyPair::generate().expect("key"),
        parent,
    )
    .expect("intermediate")
}

/// A leaf for `localhost`, good for both server and client authentication.
struct Leaf {
    cert_pem: String,
    key_pem: String,
}

fn leaf(common_name: &str, issuer: &Issuer) -> Leaf {
    let mut params = CertificateParams::new(vec![SERVER_NAME.to_string()]).expect("leaf params");
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let key = KeyPair::generate().expect("key");
    let cert = params.signed_by(&key, issuer).expect("leaf");
    Leaf {
        cert_pem: cert.pem(),
        key_pem: key.serialize_pem(),
    }
}

/// How a leaf relates to the trusted root.
#[derive(Debug, Clone, Copy)]
enum Chain {
    /// `leaf <- intermediate <- trusted root`, intermediate sent.
    Valid,
    /// `leaf <- intermediate <- untrusted root`, intermediate sent. The
    /// intermediate has the same subject name as the trusted one.
    UntrustedIntermediate,
    /// `leaf <- intermediate <- trusted root`, intermediate not sent.
    MissingIntermediate,
}

/// One PKI per test: a trusted root with its intermediate, and an untrusted
/// root with an intermediate of the same name.
struct Pki {
    trusted_root: Issuer,
    trusted_intermediate: Issuer,
    rogue_intermediate: Issuer,
    dir: tempfile::TempDir,
}

impl Pki {
    fn new() -> Self {
        let trusted_root = root("krabka-test-root");
        let trusted_intermediate = intermediate("krabka-test-intermediate", &trusted_root);
        let rogue_root = root("krabka-rogue-root");
        let rogue_intermediate = intermediate("krabka-test-intermediate", &rogue_root);
        Self {
            trusted_root,
            trusted_intermediate,
            rogue_intermediate,
            dir: tempfile::tempdir().expect("tempdir"),
        }
    }

    fn write(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.dir.path().join(name);
        std::fs::write(&path, contents).expect("write PEM");
        path
    }

    fn trusted_root_path(&self) -> PathBuf {
        self.write("root.pem", &self.trusted_root.pem())
    }

    /// The leaf's chain PEM, leaf first, and its key PEM.
    fn identity(&self, common_name: &str, chain: Chain) -> (String, String) {
        let issuer = match chain {
            Chain::Valid | Chain::MissingIntermediate => &self.trusted_intermediate,
            Chain::UntrustedIntermediate => &self.rogue_intermediate,
        };
        let leaf = leaf(common_name, issuer);
        let chain_pem = match chain {
            Chain::Valid | Chain::UntrustedIntermediate => {
                format!("{}{}", leaf.cert_pem, issuer.pem())
            }
            Chain::MissingIntermediate => leaf.cert_pem,
        };
        (chain_pem, leaf.key_pem)
    }

    /// A [`TlsConfig`] whose identity is a fresh leaf with the given chain,
    /// trusting the trusted root both for servers and for client certs.
    fn tls_config(
        &self,
        common_name: &str,
        chain: Chain,
        client_auth: ClientAuthMode,
    ) -> TlsConfig {
        let (chain_pem, key_pem) = self.identity(common_name, chain);
        TlsConfig {
            cert_chain_path: self.write(&format!("{common_name}.chain.pem"), &chain_pem),
            private_key_path: self.write(&format!("{common_name}.key.pem"), &key_pem),
            trust_roots_path: Some(self.trusted_root_path()),
            client_ca_path: Some(self.trusted_root_path()),
            client_auth,
        }
    }
}

/// The side whose certificate check ended the handshake.
#[derive(Debug, Clone, PartialEq)]
enum Rejected {
    ByClient(rustls::Error),
    ByServer(rustls::Error),
}

/// Runs a full handshake between `client` and `server` over in-memory buffers.
fn handshake(client: Arc<ClientConfig>, server: Arc<ServerConfig>) -> Result<(), Rejected> {
    let name = ServerName::try_from(SERVER_NAME).expect("server name");
    let mut client = ClientConnection::new(client, name).expect("client connection");
    let mut server = ServerConnection::new(server).expect("server connection");
    while client.is_handshaking() || server.is_handshaking() {
        let mut moved = false;
        while client.wants_write() {
            let mut bytes = Vec::new();
            client.write_tls(&mut bytes).expect("client write");
            server.read_tls(&mut bytes.as_slice()).expect("server read");
            server.process_new_packets().map_err(Rejected::ByServer)?;
            moved = true;
        }
        while server.wants_write() {
            let mut bytes = Vec::new();
            server.write_tls(&mut bytes).expect("server write");
            client.read_tls(&mut bytes.as_slice()).expect("client read");
            client.process_new_packets().map_err(Rejected::ByClient)?;
            moved = true;
        }
        assert2::assert!(moved, "handshake stalled");
    }
    Ok(())
}

fn install_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

fn unknown_issuer() -> rustls::Error {
    rustls::Error::InvalidCertificate(CertificateError::UnknownIssuer)
}

/// The client-side builders that verify a server certificate.
#[derive(Debug, Clone, Copy)]
enum ClientBuilder {
    TrustRoots,
    FromPem,
    WithIdentity,
}

impl ClientBuilder {
    const ALL: [Self; 3] = [Self::TrustRoots, Self::FromPem, Self::WithIdentity];

    fn build(self, pki: &Pki) -> Arc<ClientConfig> {
        let config = pki.tls_config("client", Chain::Valid, ClientAuthMode::Disabled);
        match self {
            Self::TrustRoots => config.build_client_config(),
            Self::FromPem => {
                let (chain_pem, key_pem) = pki.identity("client-pem", Chain::Valid);
                TlsConfig::build_client_config_from_pem(
                    chain_pem.as_bytes(),
                    key_pem.as_bytes(),
                    pki.trusted_root.pem().as_bytes(),
                )
            }
            Self::WithIdentity => config.build_client_config_with_identity(),
        }
        .expect("client config")
    }
}

#[test]
fn client_configs_verify_the_server_chain() {
    install_provider();
    for builder in ClientBuilder::ALL {
        for (chain, want) in [
            (Chain::Valid, Ok(())),
            (
                Chain::UntrustedIntermediate,
                Err(Rejected::ByClient(unknown_issuer())),
            ),
            (
                Chain::MissingIntermediate,
                Err(Rejected::ByClient(unknown_issuer())),
            ),
        ] {
            let pki = Pki::new();
            let server = pki
                .tls_config("server", chain, ClientAuthMode::Disabled)
                .build_server_config()
                .expect("server config");
            let got = handshake(builder.build(&pki), server);
            assert2::assert!(got == want, "{builder:?} with a {chain:?} server chain");
        }
    }
}

#[test]
fn required_client_auth_verifies_the_client_chain() {
    install_provider();
    for (chain, want) in [
        (Chain::Valid, Ok(())),
        (
            Chain::UntrustedIntermediate,
            Err(Rejected::ByServer(unknown_issuer())),
        ),
        (
            Chain::MissingIntermediate,
            Err(Rejected::ByServer(unknown_issuer())),
        ),
    ] {
        let pki = Pki::new();
        let server = pki
            .tls_config("server", Chain::Valid, ClientAuthMode::Required)
            .build_server_config()
            .expect("server config");
        let client = pki
            .tls_config("client", chain, ClientAuthMode::Disabled)
            .build_client_config_with_identity()
            .expect("client config");
        let got = handshake(client, server);
        assert2::assert!(got == want, "a {chain:?} client chain");
    }
}
