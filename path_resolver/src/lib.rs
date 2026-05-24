use thiserror::Error;

#[derive(Debug, Error)]
pub enum PathResolverError {
    #[error("invalid path")]
    InvalidPath,
}

#[derive(Debug, Clone)]
pub struct ResolvedPath {
    pub canonical_path: String,
    pub object_id: String,
}

pub trait PathResolver: Send + Sync {
    fn resolve(&self, path: &str) -> Result<ResolvedPath, PathResolverError>;
}

pub struct DefaultPathResolver;

impl DefaultPathResolver {
    pub fn new() -> Self {
        Self
    }
}

impl PathResolver for DefaultPathResolver {
    fn resolve(&self, path: &str) -> Result<ResolvedPath, PathResolverError> {
        let canonical = normalize(path)?;
        let object_id = format!("oid:{}", blake3::hash(canonical.as_bytes()).to_hex());
        Ok(ResolvedPath {
            canonical_path: canonical,
            object_id,
        })
    }
}

fn normalize(path: &str) -> Result<String, PathResolverError> {
    if path.is_empty() || !path.starts_with('/') || path.contains('\0') {
        return Err(PathResolverError::InvalidPath);
    }

    let mut parts = Vec::new();
    for part in path.split('/') {
        if part.is_empty() || part == "." {
            continue;
        }
        if part == ".." {
            return Err(PathResolverError::InvalidPath);
        }
        parts.push(part);
    }

    if parts.is_empty() {
        return Ok("/".to_string());
    }

    Ok(format!("/{}", parts.join("/")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_canonicalizes_and_hashes() {
        let resolver = DefaultPathResolver::new();
        let r = resolver.resolve("//a///b").expect("resolve should work");
        assert_eq!(r.canonical_path, "/a/b");
        assert!(r.object_id.starts_with("oid:"));
    }

    #[test]
    fn reject_invalid_paths() {
        let resolver = DefaultPathResolver::new();
        assert!(resolver.resolve("a").is_err());
        assert!(resolver.resolve("/a/../b").is_err());
    }
}
