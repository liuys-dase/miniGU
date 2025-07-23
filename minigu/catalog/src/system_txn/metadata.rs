use std::fmt::{self, Display};
use serde::{Deserialize, Serialize};

/// Key type for identifying metadata entries in the catalog
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MetadataKey {
    Directory(String),
    Schema(String),
    Graph(String),
    GraphType(String),
    VertexType(String),
    EdgeType(String),
}

impl Display for MetadataKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetadataKey::Directory(name) => write!(f, "directory:{}", name),
            MetadataKey::Schema(name) => write!(f, "schema:{}", name),
            MetadataKey::Graph(name) => write!(f, "graph:{}", name),
            MetadataKey::GraphType(name) => write!(f, "graph_type:{}", name),
            MetadataKey::VertexType(name) => write!(f, "vertex_type:{}", name),
            MetadataKey::EdgeType(name) => write!(f, "edge_type:{}", name),
        }
    }
}

/// Directory metadata structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryMetadata {
    pub name: String,
}

/// Schema metadata structure  
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaMetadata {
    pub name: String,
}

/// Value type for storing metadata in the catalog
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetadataValue {
    Directory(DirectoryMetadata),
    Schema(SchemaMetadata),
    Graph(GraphMetadata),
    GraphType(GraphTypeMetadata),
    VertexType(VertexTypeMetadata),
    EdgeType(EdgeTypeMetadata),
}

/// Graph metadata structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphMetadata {
    pub name: String,
    pub schema_name: Option<String>,
    pub properties: Vec<String>,
}

/// Graph type metadata structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphTypeMetadata {
    pub name: String,
    pub vertex_types: Vec<String>,
    pub edge_types: Vec<String>,
}

/// Vertex type metadata structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VertexTypeMetadata {
    pub name: String,
    pub properties: Vec<PropertyMetadata>,
}

/// Edge type metadata structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeTypeMetadata {
    pub name: String,
    pub source_vertex_type: String,
    pub target_vertex_type: String,
    pub properties: Vec<PropertyMetadata>,
}

/// Property metadata structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PropertyMetadata {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

impl MetadataValue {
    /// Get the name of the metadata entity
    pub fn name(&self) -> &str {
        match self {
            MetadataValue::Directory(d) => &d.name,
            MetadataValue::Schema(s) => &s.name,
            MetadataValue::Graph(g) => &g.name,
            MetadataValue::GraphType(gt) => &gt.name,
            MetadataValue::VertexType(vt) => &vt.name,
            MetadataValue::EdgeType(et) => &et.name,
        }
    }

    /// Check if this metadata value is compatible with the given key
    pub fn is_compatible_with_key(&self, key: &MetadataKey) -> bool {
        matches!(
            (self, key),
            (MetadataValue::Directory(_), MetadataKey::Directory(_))
                | (MetadataValue::Schema(_), MetadataKey::Schema(_))
                | (MetadataValue::Graph(_), MetadataKey::Graph(_))
                | (MetadataValue::GraphType(_), MetadataKey::GraphType(_))
                | (MetadataValue::VertexType(_), MetadataKey::VertexType(_))
                | (MetadataValue::EdgeType(_), MetadataKey::EdgeType(_))
        )
    }
}