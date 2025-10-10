use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use minigu_common::data_type::{DataSchemaRef, LogicalType};
use minigu_common::types::{LabelId, PropertyId};

use crate::error::CatalogResult;
use crate::label_set::LabelSet;
use crate::property::Property;
use crate::txn::ReadView;

pub type DirectoryRef = Arc<dyn DirectoryProvider>;
pub type SchemaRef = Arc<dyn SchemaProvider>;
pub type GraphRef = Arc<dyn GraphProvider>;
pub type GraphTypeRef = Arc<dyn GraphTypeProvider>;
pub type VertexTypeRef = Arc<dyn VertexTypeProvider>;
pub type EdgeTypeRef = Arc<dyn EdgeTypeProvider>;
pub type ProcedureRef = Arc<dyn ProcedureProvider>;

/// The top-level catalog provider, responsible for managing multiple directories and schemas,
/// resembling a UNIX filesystem.
pub trait CatalogProvider: Debug + Send + Sync {
    /// Retrieves the root directory or schema of the catalog.
    fn get_root(&self) -> CatalogResult<DirectoryOrSchema>;
}

pub trait DirectoryProvider: Debug + Send + Sync + 'static {
    /// Returns the parent directory ID of the directory.
    fn parent(&self) -> Option<DirectoryRef>;

    /// Retrieves a child directory or schema by its name.
    fn get_child(&self, name: &str) -> CatalogResult<Option<DirectoryOrSchema>>;

    /// Retrieves a child directory or schema by its name with a read view.
    /// Default implementation ignores the view and delegates to `get_child`.
    fn get_child_with(
        &self,
        name: &str,
        _view: &ReadView,
    ) -> CatalogResult<Option<DirectoryOrSchema>> {
        self.get_child(name)
    }

    /// Returns the names of the children of the directory.
    fn children_names(&self) -> Vec<String> {
        self.children_names_with(&ReadView::latest())
    }

    /// Returns the names of the children with a read view.
    /// Default implementation falls back to `children_names()`.
    fn children_names_with(&self, _view: &ReadView) -> Vec<String> {
        self.children_names()
    }

    /// Downcast support for concrete implementations
    fn as_any(&self) -> &dyn Any;
}

/// Represents a logical schema, which contains graphs and graph type definitions.
pub trait SchemaProvider: Debug + Send + Sync + 'static {
    /// Returns the parent directory ID of the schema.
    fn parent(&self) -> Option<DirectoryRef>;

    /// Returns the names of the graphs in the schema.
    fn graph_names(&self) -> Vec<String> {
        self.graph_names_with(&ReadView::latest())
    }

    /// Returns the names of the graphs in the schema with a read view.
    /// Default implementation falls back to `graph_names()`.
    fn graph_names_with(&self, _view: &ReadView) -> Vec<String> {
        self.graph_names()
    }

    /// Retrieves a graph by its name.
    fn get_graph(&self, name: &str) -> CatalogResult<Option<GraphRef>>;

    /// Retrieves a graph by its name with a read view.
    /// Default implementation ignores the view and delegates to `get_graph`.
    fn get_graph_with(&self, name: &str, _view: &ReadView) -> CatalogResult<Option<GraphRef>> {
        self.get_graph(name)
    }

    /// Returns the names of the graph types in the schema.
    fn graph_type_names(&self) -> Vec<String> {
        self.graph_type_names_with(&ReadView::latest())
    }

    /// Returns the names of the graph types in the schema with a read view.
    /// Default implementation falls back to `graph_type_names()`.
    fn graph_type_names_with(&self, _view: &ReadView) -> Vec<String> {
        self.graph_type_names()
    }

    /// Retrieves a graph type by its name.
    fn get_graph_type(&self, name: &str) -> CatalogResult<Option<GraphTypeRef>>;

    /// Retrieves a graph type by its name with a read view.
    /// Default implementation ignores the view and delegates to `get_graph_type`.
    fn get_graph_type_with(
        &self,
        name: &str,
        _view: &ReadView,
    ) -> CatalogResult<Option<GraphTypeRef>> {
        self.get_graph_type(name)
    }

    /// Returns the names of the procedures in the schema.
    fn procedure_names(&self) -> Vec<String> {
        self.procedure_names_with(&ReadView::latest())
    }

    /// Returns the names of the procedures in the schema with a read view.
    /// Default implementation falls back to `procedure_names()`.
    fn procedure_names_with(&self, _view: &ReadView) -> Vec<String> {
        self.procedure_names()
    }

    /// Retrieves a procedure by its name.
    fn get_procedure(&self, name: &str) -> CatalogResult<Option<ProcedureRef>>;

    /// Retrieves a procedure by its name with a read view.
    /// Default implementation ignores the view and delegates to `get_procedure`.
    fn get_procedure_with(
        &self,
        name: &str,
        _view: &ReadView,
    ) -> CatalogResult<Option<ProcedureRef>> {
        self.get_procedure(name)
    }

    /// Downcast support for concrete implementations
    fn as_any(&self) -> &dyn Any;
}

/// Represents a graph, which is an instance of a graph type.
///
/// Use [`Arc::downcast`] to cast the trait object into the concrete type.
pub trait GraphProvider: Debug + Send + Sync + Any {
    /// Returns the graph type of the graph.
    fn graph_type(&self) -> GraphTypeRef;

    /// Returns a reference to the underlying graph.
    fn as_any(&self) -> &dyn Any;
}

/// Represents a graph type, which defines the structure of a graph.
/// It contains vertex types and edge types.
pub trait GraphTypeProvider: Debug + Send + Sync {
    /// Retrieves the ID of a label by its name using latest view by default.
    fn get_label_id(&self, name: &str) -> CatalogResult<Option<LabelId>> {
        self.get_label_id_with(name, &ReadView::latest())
    }

    /// Retrieves the ID of a label by its name with a read view.
    fn get_label_id_with(&self, name: &str, view: &ReadView) -> CatalogResult<Option<LabelId>>;

    /// Returns the names of the labels in the graph type using latest view by default.
    fn label_names(&self) -> Vec<String> {
        self.label_names_with(&ReadView::latest())
    }

    /// Returns the names of the labels in the graph type with a read view.
    fn label_names_with(&self, view: &ReadView) -> Vec<String>;

    /// Retrieves a vertex type by its key label set using latest view by default.
    fn get_vertex_type(&self, key: &LabelSet) -> CatalogResult<Option<VertexTypeRef>> {
        self.get_vertex_type_with(key, &ReadView::latest())
    }

    /// Retrieves a vertex type by its key label set with a read view.
    fn get_vertex_type_with(
        &self,
        key: &LabelSet,
        view: &ReadView,
    ) -> CatalogResult<Option<VertexTypeRef>>;

    /// Returns the keys of the vertex types in the graph type using latest view by default.
    fn vertex_type_keys(&self) -> Vec<LabelSet> {
        self.vertex_type_keys_with(&ReadView::latest())
    }

    /// Returns the keys of the vertex types in the graph type with a read view.
    fn vertex_type_keys_with(&self, view: &ReadView) -> Vec<LabelSet>;

    /// Retrieves an edge type by its key label set using latest view by default.
    fn get_edge_type(&self, key: &LabelSet) -> CatalogResult<Option<EdgeTypeRef>> {
        self.get_edge_type_with(key, &ReadView::latest())
    }

    /// Retrieves an edge type by its key label set with a read view.
    fn get_edge_type_with(
        &self,
        key: &LabelSet,
        view: &ReadView,
    ) -> CatalogResult<Option<EdgeTypeRef>>;

    /// Returns the keys of the edge types in the graph type using latest view by default.
    fn edge_type_keys(&self) -> Vec<LabelSet> {
        self.edge_type_keys_with(&ReadView::latest())
    }

    /// Returns the keys of the edge types in the graph type with a read view.
    fn edge_type_keys_with(&self, view: &ReadView) -> Vec<LabelSet>;
}

/// Represents a vertex type, which defines the structure of a vertex.
pub trait VertexTypeProvider: Debug + Send + Sync + PropertiesProvider {
    /// Returns the label set of the vertex type.
    fn label_set(&self) -> LabelSet;
}

/// Represents an edge type, which defines the structure of an edge.
pub trait EdgeTypeProvider: Debug + Send + Sync + PropertiesProvider {
    /// Returns the label set of the edge type.
    fn label_set(&self) -> LabelSet;

    /// Returns the source vertex type of the edge type.
    fn src(&self) -> VertexTypeRef;

    /// Returns the destination vertex type of the edge type.
    fn dst(&self) -> VertexTypeRef;
}

/// Represents a property set, which contains properties of a vertex or edge type.
pub trait PropertiesProvider: Debug + Send + Sync {
    /// Retrieves a property by its name.
    fn get_property(&self, name: &str) -> CatalogResult<Option<(PropertyId, &Property)>>;

    /// Returns the properties of the property set.
    fn properties(&self) -> Vec<(PropertyId, Property)>;
}

/// Represents the metadata of a procedure.
pub trait ProcedureProvider: Debug + Send + Sync + Any {
    /// Returns the parameters of the procedure.
    fn parameters(&self) -> &[LogicalType];

    /// Returns the data schema of the procedure.
    fn schema(&self) -> Option<DataSchemaRef>;

    /// Returns a reference to the underlying procedure.
    fn as_any(&self) -> &dyn Any;
}

#[derive(Debug, Clone)]
pub enum DirectoryOrSchema {
    Directory(DirectoryRef),
    Schema(SchemaRef),
}

impl DirectoryOrSchema {
    #[inline]
    pub fn parent(&self) -> Option<DirectoryRef> {
        match self {
            Self::Directory(dir) => dir.parent(),
            Self::Schema(schema) => schema.parent(),
        }
    }

    #[inline]
    pub fn is_directory(&self) -> bool {
        matches!(self, Self::Directory(_))
    }

    #[inline]
    pub fn is_schema(&self) -> bool {
        matches!(self, Self::Schema(_))
    }

    #[inline]
    pub fn into_directory(self) -> Option<DirectoryRef> {
        match self {
            Self::Directory(dir) => Some(dir),
            Self::Schema(_) => None,
        }
    }

    #[inline]
    pub fn into_schema(self) -> Option<SchemaRef> {
        match self {
            Self::Directory(_) => None,
            Self::Schema(schema) => Some(schema),
        }
    }

    #[inline]
    pub fn as_directory(&self) -> Option<&DirectoryRef> {
        match self {
            Self::Directory(dir) => Some(dir),
            Self::Schema(_) => None,
        }
    }

    #[inline]
    pub fn as_schema(&self) -> Option<&SchemaRef> {
        match self {
            Self::Directory(_) => None,
            Self::Schema(schema) => Some(schema),
        }
    }
}

impl From<DirectoryRef> for DirectoryOrSchema {
    #[inline]
    fn from(value: DirectoryRef) -> Self {
        Self::Directory(value)
    }
}

impl From<SchemaRef> for DirectoryOrSchema {
    #[inline]
    fn from(value: SchemaRef) -> Self {
        Self::Schema(value)
    }
}
